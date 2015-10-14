using System.Collections.Generic;

namespace System.Data.MonetDb
{
    internal class MonetDbParameterCollection : List<IDbDataParameter>, IDataParameterCollection
    {
        public bool Contains(string parameterName)
        {
            var parameter = Find(delegate(IDbDataParameter param)
            {
                return param.ParameterName.Equals(parameterName);
            });

            return parameter != null;
        }

        public int IndexOf(string parameterName)
        {
            var index = FindIndex(delegate(IDbDataParameter param)
            {
                return param.ParameterName.Equals(parameterName);
            });

            return index;
        }

        public void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index > -1)
                RemoveAt(index);
        }

        public object this[string parameterName]
        {
            get
            {
                var index = IndexOf(parameterName);
                if (index > -1)
                    return this[index];
                else
                    return null;
            }
            set
            {
                var index = IndexOf(parameterName);
                if (index > -1)
                    this[index] = (IDbDataParameter)value;
                else
                    Add((IDbDataParameter)value);
            }
        }
    }

    internal class MonetDbParameter : IDbDataParameter
    {
        internal string GetProperParameter()
        {
            if (Value == DBNull.Value)
                return "NULL";
            //  If it is a string then let's sanitize the quotes and enclose the string in quotes
            if (Value is string)
                return @"'" + Value.ToString().Replace(@"'", @"''").Replace(@"""", @"""""") + @"'";

            return Value.ToString();
        }

        #region IDbDataParameter Members

        public byte Precision
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public byte Scale
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public int Size
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        #endregion

        #region IDataParameter Members

        public DbType DbType
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public ParameterDirection Direction
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public bool IsNullable
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        public string ParameterName
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public string SourceColumn
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public DataRowVersion SourceVersion
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public object Value { get; set; }

        #endregion
    }
}
