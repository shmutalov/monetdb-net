/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is MonetDB .NET Client Library.
 * 
 * The Initial Developer of the Original Code is Tim Gebhardt <tim@gebhardtcomputing.com>.
 * Portions created by Tim Gebhardt are Copyright (C) 2007. All Rights Reserved.
 */

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
        public static string TestConnectionString = "host=127.0.0.1;port=50000;username=voc;password=voc;database=demo;ssl=false;";

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

            try
            {
                conn = new MonetDbConnection(TestConnectionString.Replace("ssl=false", "ssl=true"));
                conn.Open();
            }
            catch (MonetDbException ex)
            {
                if (ex.Message.IndexOf("not supported", StringComparison.InvariantCultureIgnoreCase) < 0)
                    throw;
            }
            finally
            {
                conn.Close();
            }
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

        [Test]
        public void TestConnectWrongDatabase()
        {
            MonetDbConnection conn = new MonetDbConnection("host=localhost;port=50000;username=voc;password=voc;database=wrong");
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestConnectNoDatabase()
        {
            new MonetDbConnection("host=localhost;port=50000;username=voc;password=voc");
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
        [ExpectedException(typeof(MonetDbException), UserMessage="This should throw a message that the database doesn't exist, but it's successfully changing the database name and reconnecting if it's doing so")]
        public void TestChangeDatabase()
        {
            MonetDbConnection conn = new MonetDbConnection(TestConnectionString);
            conn.Open();
            Assert.IsTrue(conn.State == System.Data.ConnectionState.Open);

            conn.ChangeDatabase("somethingelse");
        }

        [Test]
        public void TestConnectionPooling()
        {
            //This test is intended to be run through a debugger and see if the connection pooling is 
            //dynamically creating and destroying the connection pools.
            //Only run this test, because the other tests will mess up the connection pool settings...
            //I know it's not very TDD and this is a code smell, but this is pretty standard fare for
            //database connectivity.
            string modifiedConnString = TestConnectionString + "poolminimum=1;poolmaximum=5;";
            MonetDbConnection[] conns = new MonetDbConnection[5];
            for (int i = 0; i < conns.Length; i++)
            {
                conns[i] = new MonetDbConnection(modifiedConnString);
                conns[i].Open();
            }

            for (int i = 0; i < conns.Length; i++)
            {
                conns[i].Close();
            }
        }
    }
}
