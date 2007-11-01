using System;
using System.Collections.Generic;
using System.Text;

using MonetDb;

using NUnit.Framework;

namespace MonetDbTest
{
    [TestFixture]
    public class MonetDbConnectionTest
    {
        /// <summary>
        /// This connection string is based on the VOC test database that can be found 
        /// on the MonetDB website and loading it into the "demo" database.  Following
        /// the "Getting Started" instructions on the MonetDB website will set up the environment 
        /// correctly for these tests to run.
        /// </summary>
        public static string TestConnectionString = "host=localhost;port=50000;username=voc;password=voc;database=demo;ssl=false";

        [Test]
        public void TestConnect()
        {
            MonetDbConnection conn = new MonetDbConnection();
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Closed);

            try
            {
                conn.Open();
            }
            catch (InvalidOperationException)
            { }

            conn = new MonetDbConnection(TestConnectionString);
            conn.Open();
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Open);
            Assert.AreEqual(conn.Database, "demo");
            conn.Close();
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Closed);
            Assert.AreEqual(conn.Database, "demo");
            
            conn = new MonetDbConnection(TestConnectionString.Replace("ssl=false", "ssl=true"));
            conn.Open();
            conn.Close();
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConnectMalformed1()
        {
            new MonetDbConnection(TestConnectionString.Replace("port=50000", "port=asb"));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConnectMalformed2()
        {
            new MonetDbConnection(TestConnectionString.Replace("port=50000", "port"));
        }

        /// <summary>
        /// Connecting to a non-existant database works if there is only one database.
        /// </summary>
        [Test]
        public void TestConnectWrongDatabase()
        {
            MonetDbConnection conn = new MonetDbConnection("host=localhost;port=50000;username=voc;password=voc;database=wrong;ssl=true");
            conn.Open();
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConnectNoDatabase()
        {
            new MonetDbConnection("host=localhost;port=50000;username=voc;password=voc;ssl=true");
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestConnectDoubleOpen()
        {
            MonetDbConnection conn = new MonetDbConnection(TestConnectionString);
            conn.Open();
            conn.Open();
        }

        [Test]
        public void TestChangeDatabase()
        {
            MonetDbConnection conn = new MonetDbConnection(TestConnectionString);
            conn.Open();
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Open);

            conn.ChangeDatabase("somethingelse");
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Open);
            Assert.AreEqual(conn.ConnectionString, TestConnectionString.Replace("database=demo", "database=somethingelse"));
            conn.Close();
        }
        
    }
}
