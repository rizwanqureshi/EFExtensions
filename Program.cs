using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;
using EFExtensions;


namespace EFExtensionsDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //Get EmployeeDBContext
                var myDbContext = new EmployeeDBContext();

                //Add few employees
                for (int i = 0; i < 5000; i++)
                {
                    myDbContext.Employees.Add(new Employee { FirstName = "Rizwan", LastName = "Qureshi", DOB = new DateTime(1980, 4, 3), ID = i });
                }

                //Let's see how much does it take
                var startTime = DateTime.Now;               
                myDbContext.BulkLoad<Employee>(BulkLoadOptions.FirstDeleteThenInsert);
                Console.WriteLine("{0} - Records are inserted in {1} milliseconds", myDbContext.Employees.Count(), DateTime.Now.Subtract(startTime).TotalMilliseconds );
            }

            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            
            {
                Console.ReadLine();
            
            }
            
        }
    }


    // The Employee Entity
    public class Employee
	{
        public int ID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public DateTime DOB { get; set; }
		
	}

    //The Employee DbContext
    public class EmployeeDBContext : DbContext
    {
        public EmployeeDBContext()
            : base("server=localhost;initial catalog=EmployeeDB;integrated security=True")
        {
        
        }
        
        public DbSet<Employee> Employees { get; set; }        

    }



}
