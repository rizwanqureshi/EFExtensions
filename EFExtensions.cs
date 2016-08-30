using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Xml;
using System.Xml.Linq;
using System.IO;
using System.Xml.Serialization;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Common;
using System.Data.Entity.Migrations;
using System.Data.Entity.Validation;
using System.Data.Entity.Core;
using System.Data.Entity.Core.Mapping;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Exceptions;
using EntityFramework.MappingAPI.Extensions;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data.Entity.Core.Objects;
using System.Configuration;


namespace EFExtensions
{

    /// <summary>
    /// Provides different methods for bulk load
    /// </summary>    
    public enum BulkLoadOptions
    {
        /// <summary>
        ///Delete all records before bulk insert    
        /// </summary>
        FirstDeleteThenInsert,
        /// <summary>
        /// Differentially updates target table with source table.
        /// All the existing rows are updaetd, new rows are inserted      
        /// </summary>
        UpsertWithoutDelete,
        /// <summary>
        /// Differentially updates target table with source table.
        /// All the existing rows are updaetd, new rows are inserted and 
        /// the missing record in target will be deleted 
        /// </summary>
        UpsertWithDelete,
        /// <summary>
        /// Delete all records in destination if matches with source
        /// </summary>
        Delete,

    }

   /// <summary>
   /// Author: Rizwan Qureshi
   /// Date: 30-August-2016
   /// EF Extensions are meant to provide some missing features, which are required by folks to 
   /// do the bulk updates
   /// </summary>
    public static class EFExtensions
    {
        /// <summary>
        /// This extension method proivdes bulk load opreation for DBContext by using SqlBulkCopy
        /// </summary>
        /// <typeparam name="T">Entity Type</typeparam>
        /// <param name="db">Database Context</param>
        /// <param name="options">BulkLodOptions: FirstDeleteThenInsert | UpsertWithoutDelete | UpsertWithDelete| UpsertWithDelete</param>
        public static void BulkLoad<T>(this DbContext db, BulkLoadOptions options)
        {
            try
            {
                #region get dbset
                var dbSet = db.Set(typeof(T));
                #endregion

                #region get table name
                var mapping = db.Db<T>();
                string tableName = mapping.TableName;
                string tempTableName = string.Concat(tableName, "_temp");
                #endregion

                #region convert entity to table
                //remove navigation properties if they are not FK
                var properties = mapping.Properties.Where(x => !(x.IsNavigationProperty && !x.IsFk)).ToList();


                //create datatable from entity properties
                DataTable dt = new DataTable();
                foreach (var property in properties)
                {
                    if (property.IsNavigationProperty && property.IsFk)
                        dt.Columns.Add(property.ColumnName, db.Db(property.Type).Pks[0].Type);
                    else
                        dt.Columns.Add(property.ColumnName, property.Type);
                }


                //insert values into datatable from entity properties' values
                foreach (var item in dbSet.Local)
                {
                    DataRow row = dt.NewRow();
                    foreach (var property in properties)
                    {

                        //if it is navigation property then:
                        if (property.IsNavigationProperty && property.IsFk)
                        {
                            //1- get the mapped entity 
                            var NavigationMappedObject = item.GetType().GetProperty(property.PropertyName).GetValue(item);

                            //2- find it primary key
                            var NavigationMappedObjectMapping = db.Db(NavigationMappedObject.GetType());
                            var NavigationPkMapping = NavigationMappedObjectMapping.Pks[0];

                            //3- set the primary key value in navigation column of data table
                            row[property.ColumnName] = property.Type.GetProperty(NavigationPkMapping.PropertyName).GetValue(NavigationMappedObject);
                        }

                        else
                        {
                            row[property.ColumnName] = item.GetType().GetProperty(property.PropertyName).GetValue(item) ?? DBNull.Value;
                        }
                    }
                    dt.Rows.Add(row);
                }
                #endregion

                if (options == BulkLoadOptions.FirstDeleteThenInsert)
                {
                    #region delete records and bulk copy data to target table

                    //bulk coying 
                    using (var sqlConnection = new SqlConnection(db.Database.Connection.ConnectionString))
                    {
                        sqlConnection.Open();
                        using (var trans = sqlConnection.BeginTransaction())
                        {
                            try
                            {
                                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, trans);
                                //column mapping for bulk copying
                                foreach (var property in properties)
                                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(property.ColumnName, property.ColumnName));

                                //delete recrods from table            
                                string deleteRowSQL = string.Format("DELETE FROM {0}", tableName);
                                db.Database.ExecuteSqlCommand(deleteRowSQL);

                                //writing bulk records to  table
                                bulkCopy.DestinationTableName = tableName;
                                bulkCopy.WriteToServer(dt);
                                trans.Commit();
                            }

                            catch (Exception ex)
                            {
                                trans.Rollback();
                                throw new ArgumentException("Bulk load error, see the inner exception for detail", ex);
                            }
                        }
                        sqlConnection.Close();
                    }

                    #endregion
                }

                if (options == BulkLoadOptions.UpsertWithDelete)
                {
                    #region bulk copy data to temp table

                    //bulk coying 
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(db.Database.Connection.ConnectionString);

                    //column mapping for bulk copying
                    foreach (var property in properties)
                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(property.ColumnName, property.ColumnName));

                    //create temp table            
                    string tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0} \n"
                                                         + "SELECT top(0) * into {0} FROM {1}", tempTableName, tableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);

                    //writing bulk records to temp table
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.WriteToServer(dt);
                    #endregion

                    #region build merge query
                    string condition = "";
                    foreach (var Pk in mapping.Pks)
                        condition += string.Format("Target.{0}=Source.{0} AND ", Pk.ColumnName);
                    condition = condition.Remove(condition.Length - 4);

                    List<string> insertList = new System.Collections.Generic.List<string>();
                    List<string> updateList = new System.Collections.Generic.List<string>();

                    foreach (var property in properties)
                    {
                        //do not insert identity column
                        if (!property.IsIdentity)
                        insertList.Add(string.Format("[{0}]", property.ColumnName));

                        //do not update primary key
                        if(!property.IsPk)
                        updateList.Add(string.Format("TARGET.[{0}]=SOURCE.[{0}]", property.ColumnName));
                    }

                    string insertValues = string.Join(",", insertList.ToArray());
                    string updateValues = string.Join(",", updateList.ToArray());
                    var mergeQuery = string.Format("MERGE {0} AS TARGET USING {1} AS SOURCE ON {2} \n"
                                                + "WHEN NOT MATCHED BY TARGET THEN \n"
                                                + "INSERT ({3}) VALUES({3}) \n"
                                                + "WHEN MATCHED THEN \n"
                                                + "UPDATE SET {4}  \n"
                                                + "WHEN NOT MATCHED BY SOURCE THEN DELETE;"
                        //+ "OUTPUT $action, inserted.*, deleted.*;"
                                                , tableName, tempTableName, condition, insertValues, updateValues);
                    db.Database.ExecuteSqlCommand(mergeQuery);
                    #endregion

                    #region delete temp table

                    //delete temp table
                    tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0}", tempTableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);
                    #endregion
                }

                if (options == BulkLoadOptions.UpsertWithoutDelete)
                {
                    #region bulk copy data to temp table

                    //bulk coying 
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(db.Database.Connection.ConnectionString);

                    //column mapping for bulk copying
                    foreach (var property in properties)
                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(property.ColumnName, property.ColumnName));

                    //create temp table            
                    string tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0} \n"
                                                         + "SELECT top(0) * into {0} FROM {1}", tempTableName, tableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);

                    //writing bulk records to temp table
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.WriteToServer(dt);
                    #endregion

                    #region build merge query
                    string condition = "";
                    foreach (var Pk in mapping.Pks)
                        condition += string.Format("Target.{0}=Source.{0} AND ", Pk.ColumnName);
                    condition = condition.Remove(condition.Length - 4);

                    List<string> insertList = new System.Collections.Generic.List<string>();
                    List<string> updateList = new System.Collections.Generic.List<string>();

                    foreach (var property in properties)
                    {
                        //do not insert identity column
                        if (!property.IsIdentity)
                        insertList.Add(string.Format("[{0}]", property.ColumnName));
                        //do not update primary key
                        if (!property.IsPk)
                        updateList.Add(string.Format("TARGET.[{0}]=SOURCE.[{0}]", property.ColumnName));
                    }

                    string insertValues = string.Join(",", insertList.ToArray());
                    string updateValues = string.Join(",", updateList.ToArray());
                    var mergeQuery = string.Format("MERGE {0} AS TARGET USING {1} AS SOURCE ON {2} \n"
                                                + "WHEN NOT MATCHED BY TARGET THEN \n"
                                                + "INSERT ({3}) VALUES({3}) \n"
                                                + "WHEN MATCHED THEN \n"
                                                + "UPDATE SET {4}  ;"                        
                                                , tableName, tempTableName, condition, insertValues, updateValues);
                    db.Database.ExecuteSqlCommand(mergeQuery);
                    #endregion

                    #region delete temp table

                    //delete temp table
                    tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0}", tempTableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);
                    #endregion
                }

                if (options == BulkLoadOptions.Delete)
                {
                    #region bulk copy data to temp table

                    //bulk coying 
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(db.Database.Connection.ConnectionString);

                    //column mapping for bulk copying
                    foreach (var property in properties)
                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(property.ColumnName, property.ColumnName));

                    //create temp table            
                    string tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0} \n"
                                                         + "SELECT top(0) * into {0} FROM {1}", tempTableName, tableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);

                    //writing bulk records to temp table
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.WriteToServer(dt);
                    #endregion

                    #region build merge query
                    string condition = "";
                    foreach (var Pk in mapping.Pks)
                        condition += string.Format("Target.{0}=Source.{0} AND ", Pk.ColumnName);
                    condition = condition.Remove(condition.Length - 4);

                    List<string> insertList = new System.Collections.Generic.List<string>();
                    List<string> updateList = new System.Collections.Generic.List<string>();

                    foreach (var property in properties)
                    {
                        insertList.Add(string.Format("[{0}]", property.ColumnName));
                        updateList.Add(string.Format("TARGET.[{0}]=SOURCE.[{0}]", property.ColumnName));
                    }

                    string insertValues = string.Join(",", insertList.ToArray());
                    string updateValues = string.Join(",", updateList.ToArray());
                    var mergeQuery = string.Format("MERGE {0} AS TARGET USING {1} AS SOURCE ON {2} \n"
                                                 + "WHEN  MATCHED THEN DELETE;"
                        //+ "OUTPUT $action, inserted.*, deleted.*;"
                                                , tableName, tempTableName, condition, insertValues, updateValues);
                    db.Database.ExecuteSqlCommand(mergeQuery);
                    #endregion

                    #region delete temp table

                    //delete temp table
                    tempTableSQL = string.Format("IF OBJECT_ID('{0}', 'U') IS NOT NULL   DROP TABLE {0}", tempTableName);
                    db.Database.ExecuteSqlCommand(tempTableSQL);
                    #endregion
                }

            }

            catch (Exception ex)
            {

                throw new ArgumentException("Error in bulkload, see the inner exception for detail", ex);
            }
        }
    }
}


