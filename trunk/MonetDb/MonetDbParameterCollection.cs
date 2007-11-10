using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace MonetDb
{
    internal class MonetDbParameterCollection : List<IDbDataParameter>, IDataParameterCollection
    {
        public bool Contains(string parameterName)
        {
            IDbDataParameter parameter = this.Find(delegate(IDbDataParameter param)
            {
                return param.ParameterName.Equals(parameterName);
            });

            return parameter != null;
        }

        public int IndexOf(string parameterName)
        {
            int index = this.FindIndex(delegate(IDbDataParameter param)
            {
                return param.ParameterName.Equals(parameterName);
            });

            return index;
        }

        public void RemoveAt(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index > -1)
                this.RemoveAt(index);
        }

        public object this[string parameterName]
        {
            get
            {
                int index = IndexOf(parameterName);
                if (index > -1)
                    return this[index];
                else
                    return null;
            }
            set
            {
                int index = IndexOf(parameterName);
                if (index > -1)
                    this[index] = (IDbDataParameter)value;
                else
                    this.Add((IDbDataParameter)value);
            }
        }
    }

    internal class MonetDbParameter : IDbDataParameter
    {
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

        public object Value
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
    }
}
