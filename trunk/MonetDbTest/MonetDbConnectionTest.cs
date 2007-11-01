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
        [Test]
        public void TestConnect()
        {
            MonetDbConnection conn = new MonetDbConnection();
            conn.Close();

            conn = new MonetDbConnection("host=localhost;port=50000;username=voc;password=voc;database=demo;ssl=false");
            conn.Close();

            conn = new MonetDbConnection("host=localhost;port=50000;username=admin;password=sa;database=demo;ssl=true");
            conn.Close();
        }
    }
}
