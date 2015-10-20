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
using System.Data;
using System.Data.MonetDb;
using NUnit.Framework;

namespace MonetDbTest
{
    [TestFixture]
    public class MonetDbConnectionTest
    {
        private const string TestConnectionString =
            "host=127.0.0.1;port=50000;username=monetdb;password=monetdb;database=demo;";

        [Test]
        public void TestConnect()
        {
            var connection = new MonetDbConnection();
            Assert.IsTrue(connection.State == ConnectionState.Closed);

            try
            {
                connection.Open();
            }
            catch (InvalidOperationException)
            {
            }

            connection = new MonetDbConnection(TestConnectionString);
            connection.Open();
            Assert.IsTrue(connection.State == ConnectionState.Open);
            Assert.AreEqual(connection.Database, "demo");
            connection.Close();
            Assert.IsTrue(connection.State == ConnectionState.Closed);
            Assert.AreEqual(connection.Database, "demo");

            try
            {
                connection = new MonetDbConnection(TestConnectionString.Replace("ssl=false", "ssl=true"));
                connection.Open();
            }
            catch (MonetDbException ex)
            {
                if (ex.Message.IndexOf("not supported", StringComparison.InvariantCultureIgnoreCase) < 0)
                    throw;
            }
            finally
            {
                connection.Close();
            }
        }

        [Test]
        public void TestConnectMalformed1()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new MonetDbConnection(TestConnectionString.Replace("port=50000", "port=asb"));
            });
        }

        [Test]
        public void TestConnectMalformed2()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new MonetDbConnection(TestConnectionString.Replace("port=50000", "port"));
            });
        }

        [Test]
        public void TestConnectWrongDatabase()
        {
            new MonetDbConnection("host=localhost;port=50000;username=monetdb;password=monetdb;database=wrong");
        }

        [Test]
        public void TestConnectNoDatabase()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new MonetDbConnection("host=localhost;port=50000;username=monetdb;password=monetdb");
            });
        }

        [Test]
        public void TestConnectDoubleOpen()
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                var conn = new MonetDbConnection(TestConnectionString);
                conn.Open();
                conn.Open();
            });
        }

        [Test]
        public void TestChangeDatabase()
        {
            Assert.Throws<MonetDbException>(() =>
            {
                var conn = new MonetDbConnection(TestConnectionString);
                conn.Open();
                Assert.IsTrue(conn.State == ConnectionState.Open);

                conn.ChangeDatabase("demo2");
            },
                "This should throw a message that the database doesn't exist, but it's successfully changing the database name and reconnecting if it's doing so");
        }

        [Test]
        public void TestConnectionPooling()
        {
            //This test is intended to be run through a debugger and see if the connection pooling is 
            //dynamically creating and destroying the connection pools.
            //Only run this test, because the other tests will mess up the connection pool settings...
            //I know it's not very TDD and this is a code smell, but this is pretty standard fare for
            //database connectivity.
            var modifiedConnString = TestConnectionString + "poolminimum=1;poolmaximum=5;";
            var connections = new MonetDbConnection[5];

            for (var i = 0; i < connections.Length; i++)
            {
                connections[i] = new MonetDbConnection(modifiedConnString);
                connections[i].Open();
            }

            foreach (var connection in connections)
            {
                connection.Close();
            }
        }

        [Test]
        public void TestCreateInsertSelectDropTable()
        {
            // random table name
            var tableName = Guid.NewGuid().ToString();

            // random integer value
            var value = new Random().Next();

            // SQL scripts
            var createScript = string.Format("CREATE TABLE \"{0}\" (id int);", tableName);
            var insertScript = string.Format("INSERT INTO \"{0}\" VALUES({1});", tableName, value);
            var selectScript = string.Format("SELECT * FROM \"{0}\";", tableName);
            var dropScript = string.Format("DROP TABLE \"{0}\";", tableName);

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    // create table
                    command.CommandText = createScript;
                    command.ExecuteNonQuery();

                    // insert into
                    command.CommandText = insertScript;
                    // rows affected 0 or 1
                    Assert.Contains(command.ExecuteNonQuery(), new[] {0, 1});

                    // select from
                    command.CommandText = selectScript;
                    var value2 = (int) command.ExecuteScalar();
                    Assert.AreEqual(value, value2);

                    // drop table
                    command.CommandText = dropScript;
                    command.ExecuteNonQuery();
                }
            }
        }

        [Test]
        public void TestSchemaTable()
        {
            var query = "SELECT * FROM env();";

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    // create table
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var schema = reader.GetSchemaTable();

                            Assert.IsNotNull(schema);
                            Assert.IsTrue(schema.Columns.Contains("name"));
                            Assert.IsTrue(schema.Columns.Contains("value"));
                            Assert.AreEqual(typeof (string), schema.Columns["name"].DataType);
                            Assert.AreEqual(typeof (string), schema.Columns["value"].DataType);
                        }
                    }
                }
            }
        }
    }
}
