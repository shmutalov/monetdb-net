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
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace MonetDb
{
    #region Strongly Typed Pointers

    internal struct MapiConnection
    {
        public readonly IntPtr Ptr;
        public readonly int Port;
        public MapiConnection(IntPtr ptr, int port)
        {
            Ptr = ptr;
            Port = port;
        }
    }

    internal struct MapiMsg
    {
        public IntPtr Ptr;
        public MapiMsg(IntPtr ptr)
        {
            Ptr = ptr;
        }
    }

    internal struct MapiHdl
    {
        public IntPtr Ptr;
        public MapiHdl(IntPtr ptr)
        {
            Ptr = ptr;
        }
    }

    #endregion

    internal class MapiLib
    {
        private class MarshalToUtf8 : ICustomMarshaler
        {
            static MarshalToUtf8 marshaler = new MarshalToUtf8();

            public void CleanUpManagedData(object ManagedObj)
            {

            }

            public void CleanUpNativeData(IntPtr pNativeData)
            {
                Marshal.Release(pNativeData);
            }

            public int GetNativeDataSize()
            {
                return Marshal.SizeOf(typeof(byte));
            }

            public int GetNativeDataSize(IntPtr ptr)
            {
                int size = 0;
                for (size = 0; Marshal.ReadByte(ptr, size) > 0; size++)
                    ;
                return size;
            }

            public IntPtr MarshalManagedToNative(object ManagedObj)
            {
                if (ManagedObj == null)
                    return IntPtr.Zero;
                if (ManagedObj.GetType() != typeof(string))
                    throw new ArgumentException("ManagedObj", "Can only marshal type of System.String");
                byte[] array = Encoding.UTF8.GetBytes((string)ManagedObj);
                int size = Marshal.SizeOf(array[0]) * array.Length + Marshal.SizeOf(array[0]);
                IntPtr ptr = Marshal.AllocHGlobal(size);
                Marshal.Copy(array, 0, ptr, array.Length);
                Marshal.WriteByte(ptr, size - 1, 0);
                return ptr;
            }

            public object MarshalNativeToManaged(IntPtr pNativeData)
            {
                if (pNativeData == IntPtr.Zero)
                    return null;
                int size = GetNativeDataSize(pNativeData);
                byte[] array = new byte[size];
                Marshal.Copy(pNativeData, array, 0, size);
                return Encoding.UTF8.GetString(array);
            }

            public static ICustomMarshaler GetInstance(string cookie)
            {
                return marshaler;
            }
        }

        private class MarshalToUtf8Array : ICustomMarshaler
        {
            static MarshalToUtf8Array marshaler = new MarshalToUtf8Array();

            public void CleanUpManagedData(object ManagedObj)
            {

            }

            public void CleanUpNativeData(IntPtr pNativeData)
            {
                int size = GetNativeDataSize(pNativeData);
                for (int i = 0; i < size; i++)
                {
                    IntPtr ptr = Marshal.ReadIntPtr(pNativeData, i);
                    MarshalToUtf8.GetInstance(null).CleanUpNativeData(ptr);
                }
            }

            public int GetNativeDataSize()
            {
                return IntPtr.Size;
            }

            public int GetNativeDataSize(IntPtr ptr)
            {
                int size = 0;
                for (size = 0; Marshal.ReadInt32(ptr, size) > 0; size++)
                    ;
                return size;
            }

            public IntPtr MarshalManagedToNative(object ManagedObj)
            {
                if (ManagedObj == null)
                    return IntPtr.Zero;
                if (ManagedObj.GetType() != typeof(string[]))
                    throw new ArgumentException("ManagedObj", "Can only marshal type of System.String[]");
                string[] strs = (string[])ManagedObj;
                int size = IntPtr.Size * strs.Length + IntPtr.Size;
                IntPtr retval = Marshal.AllocHGlobal(size);
                for (int i = 0; i < strs.Length; i++)
                {
                    IntPtr strPtr = MarshalToUtf8.GetInstance(null).MarshalManagedToNative(strs[i]);
                    Marshal.WriteIntPtr(retval, i, strPtr);
                }
                return retval;
            }

            public object MarshalNativeToManaged(IntPtr pNativeData)
            {
                int size = GetNativeDataSize(pNativeData);
                string[] retval = new string[size];
                for (int i = 0; i < size; i++)
                {
                    IntPtr ptr = Marshal.ReadIntPtr(pNativeData, i);
                    retval[i] = MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(ptr) as string;
                }
                return retval;
            }

            public static ICustomMarshaler GetInstance(string cookie)
            {
                return marshaler;
            }
        }

        private class MarshalToUtf8ArrayArray : ICustomMarshaler
        {
            static MarshalToUtf8ArrayArray marshaler = new MarshalToUtf8ArrayArray();

            public void CleanUpManagedData(object ManagedObj)
            {

            }

            public void CleanUpNativeData(IntPtr pNativeData)
            {
                int size = GetNativeDataSize(pNativeData);
                for (int i = 0; i < size; i++)
                {
                    IntPtr ptr = Marshal.ReadIntPtr(pNativeData, i);
                    MarshalToUtf8Array.GetInstance(null).CleanUpNativeData(ptr);
                }
            }

            public int GetNativeDataSize()
            {
                return IntPtr.Size;
            }

            public int GetNativeDataSize(IntPtr ptr)
            {
                int size = 0;
                for (size = 0; Marshal.ReadInt32(ptr, size) > 0; size++)
                    ;
                return size;
            }

            public IntPtr MarshalManagedToNative(object ManagedObj)
            {
                if (ManagedObj == null)
                    return IntPtr.Zero;
                if (ManagedObj.GetType() != typeof(string[][]))
                    throw new ArgumentException("ManagedObj", "Can only marshal type of System.String[][]");
                string[] strs = (string[])ManagedObj;
                int size = IntPtr.Size * strs.Length + IntPtr.Size;
                IntPtr retval = Marshal.AllocHGlobal(size);
                for (int i = 0; i < strs.Length; i++)
                {
                    IntPtr strPtr = MarshalToUtf8Array.GetInstance(null).MarshalManagedToNative(strs[i]);
                    Marshal.WriteIntPtr(retval, i, strPtr);
                }
                return retval;
            }

            public object MarshalNativeToManaged(IntPtr pNativeData)
            {
                int size = GetNativeDataSize(pNativeData);
                string[] retval = new string[size];
                for (int i = 0; i < size; i++)
                {
                    IntPtr ptr = Marshal.ReadIntPtr(pNativeData, i);
                    retval[i] = MarshalToUtf8Array.GetInstance(null).MarshalNativeToManaged(ptr) as string;
                }
                return retval;
            }

            public static ICustomMarshaler GetInstance(string cookie)
            {
                return marshaler;
            }
        }

        #region Connecting and Disconnecting

        /// <summary>
        /// Setup a connection with a mserver at a host:port and login with username and password. 
        /// If host == NULL, the local host is accessed. If host starts with a '/' and the system 
        /// supports it, host is actually the name of a UNIX domain socket, and port is ignored. 
        /// If port == 0, a default port is used. If username == NULL, the username of the owner of 
        /// the client application containing the Mapi code is used. If password == NULL, the password 
        /// is omitted. The preferred query language is any of {sql,mil,mal,xquery }. 
        /// On success, the function returns a pointer to a structure with administration about the connection.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="lang"></param>
        /// <param name="dbname"></param>
        /// <returns></returns>
        public static MapiConnection MapiConnect(string host, int port, string username, string password, string lang, string dbname)
        {
            return new MapiConnection(
                CMapiLib.mapi_connect(host, port, username, password, lang, dbname),
                port);
        }

        /// <summary>
        /// Setup a connection with a mserver at a host:port and login with username and password. 
        /// The connection is made using the Secure Socket Layer (SSL) and hence all data transfers 
        /// to and from the server are encrypted. The parameters are the same as in mapi_connect().
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="lang"></param>
        /// <param name="dbname"></param>
        /// <returns></returns>
        public static MapiConnection MapiConnectSsl(string host, int port, string username, string password, string lang, string dbname)
        {
            return new MapiConnection(
                CMapiLib.mapi_connect_ssl(host, port, username, password, lang, dbname),
                port);
        }

        /// <summary>
        /// Terminate the session described by mid. The only possible uses of the handle after 
        /// this call is mapi_destroy() and mapi_reconnect(). Other uses lead to failure.
        /// </summary>
        /// <param name="mapi"></param>
        /// <returns></returns>
        public static MapiMsg MapiDisconnect(MapiConnection mapi)
        {
            return new MapiMsg(CMapiLib.mapi_disconnect(mapi.Ptr));
        }

        /// <summary>
        /// Terminate the session described by  mid if not already done so, and free all resources. 
        /// The handle cannot be used anymore.
        /// </summary>
        /// <param name="mapi"></param>
        /// <returns></returns>
        public static MapiMsg MapiDestroy(MapiConnection mapi)
        {
            return new MapiMsg(CMapiLib.mapi_destroy(mapi.Ptr));
        }

        /// <summary>
        /// Close the current channel (if still open) and re-establish a fresh connection. 
        /// This will remove all global session variables.
        /// </summary>
        /// <param name="mapi"></param>
        /// <returns></returns>
        public static MapiMsg MapiReconnect(MapiConnection mapi)
        {
            return new MapiMsg(CMapiLib.mapi_reconnect(mapi.Ptr));
        }

        /// <summary>
        /// Test availability of the server. Returns zero upon success.
        /// </summary>
        /// <param name="mapi"></param>
        /// <returns></returns>
        public static MapiMsg MapiPing(MapiConnection mapi)
        {
            return new MapiMsg(CMapiLib.mapi_ping(mapi.Ptr));
        }

        #endregion

        #region Sending Queries

        /// <summary>
        /// Send the Command to the database server represented by mid. 
        /// This function returns a query handle with which the results of the 
        /// query can be retrieved. The handle should be closed with mapi_close_handle(). 
        /// The command response is buffered for consumption, c.f. mapi\_fetch\_row().
        /// </summary>
        /// <param name="mapi"></param>
        /// <param name="command"></param>
        /// <returns></returns>
        public static MapiHdl MapiQuery(MapiConnection mapi, string command)
        {
            return new MapiHdl(CMapiLib.mapi_query(mapi.Ptr, command));
        }

        /// <summary>
        /// Send the Command to the database server represented by hdl, 
        /// reusing the handle from a previous query. If Command is zero it 
        /// takes the last query string kept around. The command response is 
        /// buffered for consumption, e.g. mapi_fetch_row().
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="command"></param>
        /// <returns></returns>
        public static MapiMsg MapiQueryHandle(MapiHdl hdl, string command)
        {
            return new MapiMsg(CMapiLib.mapi_query_handle(hdl.Ptr, command));
        }

        /// <summary>
        /// Send the Command to the database server replacing the placeholders (?) by the string arguments presented.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="command"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public static MapiHdl MapiQueryArray(MapiConnection mid, string command, string[] args)
        {
            return new MapiHdl(CMapiLib.mapi_query_array(mid.Ptr, command, args));
        }

        /// <summary>
        /// Similar to mapi_query(), except that the response of the server is copied immediately to the file indicated.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="command"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static MapiHdl MapiQuickQuery(MapiConnection mid, string command, FileStream fs)
        {
            return new MapiHdl(CMapiLib.mapi_quick_query(mid.Ptr, command, fs.SafeFileHandle));
        }

        /// <summary>
        /// Similar to mapi_query_array(), except that the response of the
        /// server is not analyzed, but shipped immediately to the file indicated.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="command"></param>
        /// <param name="args"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static MapiHdl MapiQuickQueryArray(MapiConnection mid, string command, string[] args, FileStream fs)
        {
            return new MapiHdl(CMapiLib.mapi_quick_query_array(mid.Ptr, command, args, fs.SafeFileHandle));
        }

        /// <summary>
        /// Send the request for processing and fetch a limited number of tuples 
        /// (determined by the window size) to assess any erroneous situation. 
        /// Thereafter, prepare for continual reading of tuples from the stream, 
        /// until an error occurs. Each time a tuple arrives, the cache is shifted one.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="command"></param>
        /// <param name="windowSize"></param>
        /// <returns></returns>
        public static MapiHdl MapiStreamQuery(MapiConnection mid, string command, int windowSize)
        {
            return new MapiHdl(CMapiLib.mapi_stream_query(mid.Ptr, command, windowSize));
        }

        /// <summary>
        /// Move the query to a newly allocated query handle (which is returned). 
        /// Possibly interact with the back-end to prepare the query for execution.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="command"></param>
        /// <returns></returns>
        public static MapiHdl MapiPrepare(MapiConnection mid, string command)
        {
            return new MapiHdl(CMapiLib.mapi_prepare(mid.Ptr, command));
        }

        /// <summary>
        /// Ship a previously prepared command to the back-end for execution. 
        /// A single answer is pre-fetched to detect any runtime error. 
        /// MOK is returned upon success.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static MapiMsg MapiExecute(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_execute(hdl.Ptr));
        }

        /// <summary>
        /// Similar to mapi\_execute but replacing the placeholders for the string values provided.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public static MapiMsg MapiExecuteArray(MapiHdl hdl, string[] args)
        {
            return new MapiMsg(CMapiLib.mapi_execute_array(hdl.Ptr, args));
        }

        /// <summary>
        /// Terminate a query. This routine is used in the rare cases that consumption of the 
        /// tuple stream produced should be prematurely terminated. It is automatically 
        /// called when a new query using the same query handle is shipped to the database 
        /// and when the query handle is closed with mapi_close_handle().
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static MapiMsg MapiFinish(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_finish(hdl.Ptr));
        }

        /// <summary>
        /// Submit a table of results to the library that can then subsequently be accessed 
        /// as if it came from the server. columns is the number of columns of the result 
        /// set and must be greater than zero. columnnames is a list of pointers to strings 
        /// giving the names of the individual columns. Each pointer may be NULL and 
        /// columnnames may be NULL if there are no names. tuplecount is the length 
        /// (number of rows) of the result set. If tuplecount is less than zero, 
        /// the number of rows is determined by a NULL pointer in the list of tuples 
        /// pointers. tuples is a list of pointers to row values. Each row value is 
        /// a list of pointers to strings giving the individual results. If one of 
        /// these pointers is NULL it indicates a NULL/nil value.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="columns"></param>
        /// <param name="columnnames"></param>
        /// <param name="columntypes"></param>
        /// <param name="columnlengths"></param>
        /// <param name="tuplecount"></param>
        /// <param name="tuples"></param>
        /// <returns></returns>
        public static MapiMsg MapiVirtualResult(MapiHdl hdl, int columns, string[] columnnames, string[] columntypes, int[] columnlengths, int tuplecount, string[][] tuples)
        {
            return new MapiMsg(CMapiLib.mapi_virtual_result(hdl.Ptr, columns, columnnames, columntypes, columnlengths, tuplecount, tuples));
        }

        #endregion

        #region Getting Results

        /// <summary>
        /// Return the number of fields in the current row.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static int MapiGetFieldCount(MapiConnection mid)
        {
            return CMapiLib.mapi_get_field_count(mid.Ptr);
        }

        /// <summary>
        /// If possible, return the number of rows in the last select call. A -1 is returned if this information is not available.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static int MapiGetRowCount(MapiConnection mid)
        {
            return CMapiLib.mapi_get_row_count(mid.Ptr);
        }

        /// <summary>
        /// Return the number of rows affected by a database update command such as SQL's INSERT/DELETE/UPDATE statements.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static int MapiRowsAffected(MapiConnection mid)
        {
            return CMapiLib.mapi_rows_affected(mid.Ptr);
        }

        /// <summary>
        /// Retrieve a row from the server. The text retrieved is kept around in a buffer linked with the 
        /// query handle from which selective fields can be extracted. It returns the number of fields 
        /// recognized. A zero is returned upon encountering end of sequence or error. This can be 
        /// analyzed in using mapi_error().
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static int MapiFetchRow(MapiHdl hdl)
        {
            return CMapiLib.mapi_fetch_row(hdl.Ptr);
        }

        /// <summary>
        /// All rows are cached at the client side first. Subsequent calls to mapi_fetch_row() will take 
        /// the row from the cache. The number or rows cached is returned.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static int MapiFetchAllRows(MapiHdl hdl)
        {
            return CMapiLib.mapi_fetch_all_rows(hdl.Ptr);
        }

        /// <summary>
        /// Read the answer to a query and pass the results verbatim to a stream. 
        /// The result is not analyzed or cached.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static int MapiQuickResponse(MapiHdl hdl, FileStream fs)
        {
            return CMapiLib.mapi_quick_response(hdl.Ptr, fs.SafeFileHandle);
        }

        /// <summary>
        /// Reset the row pointer to the requested row number. If whence is MAPI_SEEK_SET (0), 
        /// rownr is the absolute row number (0 being the first row); if whence is 
        /// MAPI_SEEK_CUR (1), rownr is relative to the current row; if whence is 
        /// MAPI\_SEEK\_END (2), rownr is relative to the last row.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="rownr"></param>
        /// <param name="whence"></param>
        /// <returns></returns>
        public static MapiMsg MapiSeekRow(MapiHdl hdl, int rownr, int whence)
        {
            return new MapiMsg(CMapiLib.mapi_seek_row(hdl.Ptr, rownr, whence));
        }

        /// <summary>
        /// Reset the row pointer to the first line in the cache. This need not be a tuple. 
        /// This is mostly used in combination with fetching all tuples at once.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static MapiMsg MapiFetchReset(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_fetch_reset(hdl.Ptr));
        }

        /// <summary>
        /// Return an array of string pointers to the individual fields. 
        /// A zero is returned upon encountering end of sequence or error. 
        /// This can be analyzed in using mapi\_error().
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static string[] MapiFetchFieldArray(MapiHdl hdl)
        {
            IntPtr ptr = CMapiLib.mapi_fetch_field_array(hdl.Ptr);
            return MarshalToUtf8Array.GetInstance(null).MarshalNativeToManaged(ptr) as string[];
        }

        /// <summary>
        /// Return a pointer a C-string representation of the value returned. 
        /// A zero is returned upon encountering an error or when the database value is NULL; 
        /// this can be analyzed in using mapi\_error().
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="fnr"></param>
        /// <returns></returns>
        public static string MapiFetchField(MapiHdl hdl, int fnr)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_fetch_field(hdl.Ptr, fnr)) as string;
        }

        /// <summary>
        /// Go to the next result set, discarding the rest of the output of the current result set.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static MapiMsg MapiNextResult(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_next_result(hdl.Ptr));
        }

        #endregion

        #region Errors

        /// <summary>
        /// Return the last error code or 0 if there is no error.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static MapiMsg MapiError(MapiConnection mid)
        {
            return new MapiMsg(CMapiLib.mapi_error(mid.Ptr));
        }

        /// <summary>
        /// Return a pointer to the last error message.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static string MapiErrorString(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_error_str(mid.Ptr)) as string;
        }

        /// <summary>
        /// Return a pointer to the last error message from the server.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static string MapiResultError(MapiHdl hdl)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_result_error(hdl.Ptr)) as string;
        }

        /// <summary>
        /// Write the error message obtained from mserver to a file.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static MapiMsg MapiExplain(MapiConnection mid, FileStream fs)
        {
            return new MapiMsg(CMapiLib.mapi_explain(mid.Ptr, fs.SafeFileHandle));
        }

        /// <summary>
        /// Write the error message obtained from mserver to a file.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static MapiMsg MapiExplainQuery(MapiHdl hdl, FileStream fs)
        {
            return new MapiMsg(CMapiLib.mapi_explain_query(hdl.Ptr, fs.SafeFileHandle));
        }

        /// <summary>
        /// Write the error message obtained from mserver to a file.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="fs"></param>
        /// <returns></returns>
        public static MapiMsg MapiExplainResult(MapiHdl hdl, FileStream fs)
        {
            return new MapiMsg(CMapiLib.mapi_explain_result(hdl.Ptr, fs.SafeFileHandle));
        }

        #endregion

        #region Parameters

        /// <summary>
        /// Bind a string variable with a field in the return table. 
        /// Upon a successful subsequent mapi_fetch_row() the indicated field 
        /// is stored in the space pointed to by val. Returns an error if 
        /// the field identified does not exist.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="fldnr"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public static MapiMsg MapiBind(MapiHdl hdl, int fldnr, string[] val)
        {
            return new MapiMsg(CMapiLib.mapi_bind(hdl.Ptr, fldnr, val));
        }

        /// <summary>
        /// Clear all field bindings.
        /// </summary>
        /// <param name="hdl"></param>
        /// <returns></returns>
        public static MapiMsg MapiClearBindings(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_clear_bindings(hdl.Ptr));
        }

        public static MapiMsg MapiClearParams(MapiHdl hdl)
        {
            return new MapiMsg(CMapiLib.mapi_clear_params(hdl.Ptr));
        }

        #endregion

        #region Miscellaneous

        /// <summary>
        /// Set the autocommit flag (default is on). 
        /// This only has an effect when the language is SQL. 
        /// In that case, the server commits after each statement sent to the server.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="autocommit"></param>
        /// <returns></returns>
        public static MapiMsg MapiSetAutocommit(MapiConnection mid, int autocommit)
        {
            return new MapiMsg(CMapiLib.mapi_setAutocommit(mid.Ptr, autocommit));
        }

        /// <summary>
        /// A limited number of tuples are pre-fetched after each execute(). 
        /// If maxrows is negative, all rows will be fetched before the application is 
        /// permitted to continue. Once the cache is filled, a number of tuples are shuffled to 
        /// make room for new ones, but taking into account non-read elements. 
        /// Filling the cache quicker than reading leads to an error.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="maxrows"></param>
        /// <returns></returns>
        public static MapiMsg MapiCacheLimit(MapiConnection mid, int maxrows)
        {
            return new MapiMsg(CMapiLib.mapi_cache_limit(mid.Ptr, maxrows));
        }

        /// <summary>
        /// Make room in the cache by shuffling percentage tuples out of the cache. 
        /// It is sometimes handy to do so, for example, when your application is stream-based 
        /// and you process each tuple as it arrives and still need a limited look-back. 
        /// This percentage can be set between 0 to 100. Making shuffle= 100% (default) 
        /// leads to paging behavior, while shuffle==1 leads to a sliding window over a 
        /// tuple stream with 1% refreshing.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="percentage"></param>
        /// <returns></returns>
        public static MapiMsg MapiCacheShuffle(MapiHdl hdl, int percentage)
        {
            return new MapiMsg(CMapiLib.mapi_cache_shuffle(hdl.Ptr, percentage));
        }

        /// <summary>
        /// Forcefully shuffle the cache making room for new rows. It ignores the read counter, so rows may be lost.
        /// </summary>
        /// <param name="hdl"></param>
        /// <param name="percentage"></param>
        /// <returns></returns>
        public static MapiMsg MapiCacheFreeup(MapiHdl hdl, int percentage)
        {
            return new MapiMsg(CMapiLib.mapi_cache_freeup(hdl.Ptr, percentage));
        }

        /// <summary>
        /// Escape special characters such as \n, \t in str with backslashes. 
        /// The returned value is a newly allocated string which should be freed by the caller.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public static string MapiQuote(string str, int size)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_quote(str, size)) as string;
        }

        /// <summary>
        /// The reverse action of mapi_quote(), turning the database representation into a C-representation. 
        /// The storage space is dynamically created and should be freed after use.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string MapiUnquote(string str)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_unquote(str)) as string;
        }

        /// <summary>
        /// Set the trace flag to monitor interaction with the server.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static MapiMsg MapiTrace(MapiConnection mid, int flag)
        {
            return new MapiMsg(CMapiLib.mapi_trace(mid.Ptr, flag));
        }

        /// <summary>
        /// Return the current value of the trace flag.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static int MapiGetTrace(MapiConnection mid)
        {
            return CMapiLib.mapi_get_trace(mid.Ptr);
        }

        /// <summary>
        /// Log the interaction between the client and server for offline inspection. 
        /// Beware that the log file overwrites any previous log. It is not intended for recovery.
        /// </summary>
        /// <param name="mid"></param>
        /// <param name="filename"></param>
        /// <returns></returns>
        public static MapiMsg MapiTraceLog(MapiConnection mid, string filename)
        {
            return new MapiMsg(CMapiLib.mapi_trace_log(mid.Ptr, filename));
        }

        #endregion

        #region Data Structure Wrappers

        public static string MapiGetName(MapiHdl hdl, int fnr)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_name(hdl.Ptr, fnr)) as string;
        }

        public static string MapiGetType(MapiHdl hdl, int fnr)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_type(hdl.Ptr, fnr)) as string;
        }

        public static string MapiGetTable(MapiHdl hdl, int fnr)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_table(hdl.Ptr, fnr)) as string;
        }

        public static int MapiGetlen(MapiConnection mid, int fnr)
        {
            return CMapiLib.mapi_get_len(mid.Ptr, fnr);
        }

        public static string MapiGetDbName(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_dbname(mid.Ptr)) as string;
        }

        public static string MapiGetHost(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_host(mid.Ptr)) as string;
        }

        public static string MapiGetUser(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_user(mid.Ptr)) as string;
        }

        public static string MapiGetLang(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_lang(mid.Ptr)) as string;
        }

        public static string MapiGetMotd(MapiConnection mid)
        {
            return MarshalToUtf8.GetInstance(null).MarshalNativeToManaged(CMapiLib.mapi_get_motd(mid.Ptr)) as string;
        }

        /// <summary>
        /// Return a list of accessible database tables.
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static string[] MapiTables(MapiConnection mid)
        {
            IntPtr ptr = CMapiLib.mapi_tables(mid.Ptr);
            return MarshalToUtf8Array.GetInstance(null).MarshalNativeToManaged(ptr) as string[];
        }

        /// <summary>
        /// Return a list of accessible tables fields. This can also be obtained by 
        /// inspecting the field descriptor returned by mapi_fetch_field().
        /// </summary>
        /// <param name="mid"></param>
        /// <returns></returns>
        public static string[] MapiFields(MapiConnection mid)
        {
            IntPtr ptr = CMapiLib.mapi_fields(mid.Ptr);
            return MarshalToUtf8Array.GetInstance(null).MarshalNativeToManaged(ptr) as string[];
        }

        #endregion

        private class CMapiLib
        {
            #region Connecting and Disconnecting

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_connect(
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string host,
                int port,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string username,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string password,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string lang,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string dbname);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_connect_ssl(
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string host,
                int port,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string username,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string password,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string lang,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string dbname);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_disconnect(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_destroy(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_reconnect(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_ping(IntPtr mid);

            #endregion

            #region Sending Queries

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_query(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_query_handle(
                IntPtr hdl,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_query_array(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] argv);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_quick_query(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command,
                Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_quick_query_array(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] argv,
                Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_stream_query(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command,
                int windowsize);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_prepare(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string command);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_execute(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_execute_array(
                IntPtr hdl,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] argv);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_finish(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_virtual_result(
                IntPtr hdl,
                int columns,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] columnnames,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] columntypes,
                int[] columnlengths,
                int tuplecount,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8ArrayArray))]string[][] tuples);

            #endregion

            #region Getting Results

            [DllImport("libMapi.dll")]
            public static extern int mapi_get_field_count(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern int mapi_get_row_count(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern int mapi_rows_affected(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern int mapi_fetch_row(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern int mapi_fetch_all_rows(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern int mapi_quick_response(
                IntPtr hdl,
                Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_seek_row(IntPtr hdl, int rownr, int whence);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_fetch_reset(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_fetch_field_array(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_fetch_field(IntPtr hdl, int fnr);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_next_result(IntPtr hdl);

            #endregion

            #region Errors

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_error(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_error_str(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_result_error(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_explain(IntPtr mid, Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_explain_query(IntPtr hdl, Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_explain_result(IntPtr hdl, Microsoft.Win32.SafeHandles.SafeFileHandle fd);

            #endregion

            #region Parameters

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_bind(
                IntPtr hdl,
                int fldnr,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8Array))]string[] val);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_clear_bindings(IntPtr hdl);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_clear_params(IntPtr hdl);

            #endregion

            #region Miscellaneous

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_setAutocommit(IntPtr mid, int autocommit);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_cache_limit(IntPtr mid, int maxrows);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_cache_shuffle(IntPtr hdl, int percentage);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_cache_freeup(IntPtr hdl, int percentage);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_quote(
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string str,
                int size);

            [DllImport("libMapi.dll", CharSet = CharSet.Auto)]
            public static extern IntPtr mapi_unquote(
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string str);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_trace(IntPtr mid, int flag);

            [DllImport("libMapi.dll")]
            public static extern int mapi_get_trace(IntPtr mid);

            [DllImport("libMapi.dll", CharSet = CharSet.Auto)]
            public static extern IntPtr mapi_trace_log(
                IntPtr mid,
                [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(MarshalToUtf8))]string fname);

            #endregion

            #region Data Structure Wrappers

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_name(IntPtr hdl, int fnr);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_type(IntPtr hdl, int fnr);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_table(IntPtr hdl, int fnr);

            [DllImport("libMapi.dll")]
            public static extern int mapi_get_len(IntPtr mid, int fnr);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_dbname(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_host(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_user(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_lang(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_get_motd(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_tables(IntPtr mid);

            [DllImport("libMapi.dll")]
            public static extern IntPtr mapi_fields(IntPtr mid);

            #endregion
        }
    }
}
