using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace DAL
{
    ///<summary>
    ///<para>Author : Nicolas Chourot (all rights reserved)</para>
    ///<para></para>
    ///<para>This class is dedicated to give access to CRUD queries to a text file organized like the following.</para>
    ///<para></para>
    ///<para>      Id | field 1 | field 2 | ... | field n</para>
    ///<para></para>
    ///<para></para>
    ///<para>The provided RecordType must be another user class which properties match the text file line fields:</para>
    ///<para>public class Record</para>
    ///<para>{</para>
    ///<para>      public int Id {get; set;}  IMPORTANT: the first member has to be of type int and named Id</para>
    ///<para>      public Type Field_name_1 {get; set;}</para>
    ///<para>      public Type Field_name_2 {get; set;}</para>
    ///<para>      ...</para>
    ///<para>      public Type Field_name_n {get; set;}</para>
    ///<para>      public Type _Excluded_Member {get; set;}</para>
    ///<para></para>
    ///<para>      public Record() {} // default constructor</para>
    ///<para>}</para>
    ///<para>All member identifiers that begin with underscore (_) will be excluded from the strongly typed binding</para>
    ///<para>with the fields of the of the target table records.</para>
    ///</summary>
    //////<typeparam name="RecordType"></typeparam>
    public class RecordsFile<RecordType>
    {
        private PropertyInfo[] RecordTypeProperties = null;
        /// <summary>
        /// Basic constructor
        /// </summary>
        /// <param name="filePath">Text file path</param>
        public RecordsFile(string filePath)
        {
            FilePath = filePath;
            object record = Activator.CreateInstance(typeof(RecordType));
            RecordTypeProperties = record.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
            ReadRecords();
        }
        /// <summary>
        /// Get the record where Id = record.Id
        /// </summary>
        /// <param name="Id"></param>
        /// <returns>Object of RecordType</returns>
        public RecordType Get(int Id)
        {
            RecordType record;
            Records.TryGetValue(Id, out record);
            return (RecordType)record;
        }
        /// <summary>
        /// Get all records
        /// </summary>
        /// <returns>List of RecordType</returns>
        public List<RecordType> ToList()
        {
            return Records.Select(d => d.Value).ToList();
        }
        /// <summary>
        /// Insert a new record
        /// </summary>
        /// <param name="record"></param>
        /// <returns>Id of the newly added record</returns>
        public int Add(RecordType record)
        {
            access.Lock();
            int Id = 0;
            try
            {
                Id = NextId;
                SetRecordId(Id, record);
                Records.Add(Id, record);
                WriteRecords();
                NextId++;
            }
            catch (Exception ex)
            {

            }
            finally
            {
                access.UnLock();
            }

            return Id;
        }
        /// <summary>
        /// Update the provided record
        /// </summary>
        /// <param name="record"></param>
        public void Update(RecordType record)
        {
            access.Lock();
            try
            {
                Records[GetRecordId(record)] = record;
                WriteRecords();
            }
            catch (Exception)
            {

            }
            finally
            {
                access.UnLock();
            }
        }
        /// <summary>
        /// Delete the record where record.Id = Id
        /// </summary>
        /// <param name="Id"></param>
        public void Delete(int Id)
        {
            try
            {
                access.Lock();
                Records.Remove(Id);
                WriteRecords();
            }
            catch (Exception)
            { }
            finally
            {
                access.UnLock();
            }
        }
        /// <summary>
        /// Delete all the records
        /// </summary>
        public void DeleteAll()
        {
            try
            {
                access.Lock();
                FlushAllRecords();
                Records.Clear();
            }
            catch (Exception)
            { }
            finally
            {
                access.UnLock();
            }
        }
        /// <summary>
        /// Get all records where record.name = value
        /// </summary>
        /// <param name="name">Name of the matching field</param>
        /// <param name="value">value to be matched</param>
        /// <returns>List of RecordType</returns>
        public List<RecordType> GetByField(string name, object value)
        {
            List<RecordType> records = new List<RecordType>();
            if ((value != null) && (FieldNameExist(name)))
            {
                foreach (RecordType record in Records.Values)
                {
                    object fieldValue = GetFieldValue(record, name);
                    if (GetFieldValue(record, name).ToString() == value.ToString())
                        records.Add(record);
                }
            }
            return records;
        }
        /// <summary>
        /// Delete all records where record.name = value
        /// </summary>
        /// <param name="name">Name of the matching field</param>
        /// <param name="value">value to be matched</param>
        /// <returns></returns>
        public void DeleteByField(string name, object value)
        {
            if ((value != null) && (FieldNameExist(name)))
            {
                try
                {
                    access.Lock();
                    bool oneHasBeenDeleted;
                    do
                    {
                        oneHasBeenDeleted = false;
                        foreach (RecordType record in Records.Values)
                        {
                            if (GetFieldValue(record, name).ToString() == value.ToString())
                            {
                                Records.Remove(GetRecordId(record));
                                oneHasBeenDeleted = true;
                                break;
                            }
                        }
                    } while (oneHasBeenDeleted);
                    WriteRecords();
                }
                catch (Exception)
                {

                }
                finally
                {
                    access.UnLock();
                }
            }
        }
        /// <summary>
        /// Get the first record where record.name = value
        /// </summary>
        /// <param name="name">Name of the matching field</param>
        /// <param name="value">value to be matched</param>
        /// <returns>RecordType</returns>
        public RecordType GetFirstByField(string name, object value)
        {
            if ((value != null) && (FieldNameExist(name)))
            {
                List<RecordType> records = GetByField(name, value);
                if (records.Count > 0)
                    return records[0];
            }
            return default(RecordType);
        }
        /// <summary>
        /// Get all records where record.name >= min and record.name <= max
        /// </summary>
        /// <param name="name">Name of the matching field</param>
        /// <param name="min">lower date range</param>
        /// <param name="max">higher date range</param>
        /// <returns>List of RecordType</returns>
        public List<RecordType> GetByPeriod(string name, DateTime min, DateTime max)
        {
            List<RecordType> records = new List<RecordType>();
            if (FieldNameExist(name))
            {
                foreach (RecordType record in Records.Values)
                {
                    DateTime fieldValue = (DateTime)GetFieldValue(record, name);
                    if ((DateTime.Compare(fieldValue, min) > 0) &&
                        (DateTime.Compare(fieldValue, max) < 0))
                        records.Add(record);
                }
            }
            return records;
        }
      

        #region private members
        private string FilePath;
        private int NextId = 0;
        private const char FIELD_SEPARATOR = '|';
        private Access access = new Access();
        private Dictionary<int, RecordType> Records = new Dictionary<int, RecordType>();
        private int GetRecordId(RecordType record)
        {
            return (int)GetFieldValue(record, "Id");
        }
        private void SetRecordId(int Id, RecordType record)
        {
            SetFieldValue(record, "Id", Id);
        }
        private object GetFieldValue(RecordType record, string name)
        {
            return record.GetType().GetProperty(name).GetValue(record, null);
        }
        private void SetFieldValue(RecordType record, string name, object value)
        {
            record.GetType().GetProperty(name).SetValue(record, value, null);
        }
        private bool FieldNameExist(string name)
        {
            return (Activator.CreateInstance(typeof(RecordType)).GetType().GetProperty(name) != null);
        }
        private object StringToObject(string s, Type type)
        {
            try
            {
                return TypeDescriptor.GetConverter(type).ConvertFrom(s);
            }
            catch (Exception)
            {
                try
                {
                    return TypeDescriptor.GetConverter(type).ConvertFrom(null, CultureInfo.CreateSpecificCulture("en-GB"), s);
                }
                catch (Exception)
                {
                    try
                    {
                        return TypeDescriptor.GetConverter(type).ConvertFrom(null, CultureInfo.CreateSpecificCulture("fr-CA"), s);
                    }
                    catch (Exception) { return null; }
                }
            }
        }
        private void ReadRecords()
        {
            access.Lock();
            StreamReader sr = null;
            try
            {
                sr = new StreamReader(FilePath);
                Records.Clear();
                while (!sr.EndOfStream)
                {
                    RecordType record = StringToRecord(sr.ReadLine());
                    if (record != null)
                    {
                        int Id = GetRecordId(record);
                        Records.Add(Id, record);
                        if (Id > NextId)
                            NextId = Id;
                    }
                }

                NextId++;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (sr != null)
                    sr.Close();
                access.UnLock();
            }
        }
        private void WriteRecords()
        {
            StreamWriter sr = null;
            try
            {
                sr = new StreamWriter(FilePath, false);
                List<string> lines = new List<string>();
                foreach (RecordType record in Records.Values)
                {
                    lines.Add(RecordToString(record).Replace('\n', ' ').Replace('\r', ' '));
                }
                foreach (string line in lines)
                {
                    sr.WriteLine(line);
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (sr != null)
                    sr.Close();
                access.UnLock();
            }
        }

        private void FlushAllRecords()
        {
            try
            {
                StreamWriter sr = new StreamWriter(FilePath, false);
                sr.Close();
            }
            catch (Exception)
            {

            }
            finally
            {
                access.UnLock();
            }

        }
        private RecordType StringToRecord(string line)
        {
            object record = null;
            try
            {
                if (!string.IsNullOrEmpty(line))
                {
                    string[] fields = line.Split(FIELD_SEPARATOR);
                    record = Activator.CreateInstance(typeof(RecordType));
                    for (int i = 0; i < fields.Length; i++)
                    {
                        if (RecordTypeProperties[i].Name[0] != '_')
                        {
                            Type fieldType = RecordTypeProperties[i].GetValue(record, null).GetType();
                            object value = StringToObject(fields[i], fieldType);
                            if (value != null)
                                RecordTypeProperties[i].SetValue(record, value, null);
                        }
                    }
                }
            }
            catch (Exception)
            {
                //throw;
                record = null;
            }
            return (RecordType)record;
        }
        private string RecordToString(RecordType record)
        {
            string recordString = "";
            try
            {
                for (int i = 0; i < RecordTypeProperties.Length; i++)
                {
                    if (RecordTypeProperties[i].Name[0] != '_')
                    {
                        if (RecordTypeProperties[i].GetValue(record, null).GetType().IsEnum)
                            recordString += ((int)RecordTypeProperties[i].GetValue(record, null)).ToString();
                        else
                            recordString += RecordTypeProperties[i].GetValue(record, null).ToString();
                        if (i < RecordTypeProperties.Length - 1)
                            recordString += FIELD_SEPARATOR;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return recordString;
        }
        #endregion
    }

    /// <summary>
    /// Memorize the info of a SQL request log
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class SQL_Journal_Item
    {
        public int Id { get; set; }
        public DateTime Time { get; set; }
        public string MethodName { get; set; }
        public string Description { get; set; }

        public SQL_Journal_Item()
        {
            Time = DateTime.Now;
            MethodName = "Unkown";
            Description = "No description";
        }
    }

    /// <summary>
    /// Used for handling a SQL requests log text file
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class SQL_Journal : DAL.RecordsFile<SQL_Journal_Item>
    {
        /// <summary>
        /// Basic constructor
        /// </summary>
        /// <param name="filePath">Text file path</param>
        public SQL_Journal(string filePath) : base(filePath)
        {
        }

        /// <summary>
        /// Add a SQL request log
        /// </summary>
        /// <param name="Method">Name of the member function where the SQL request is executed</param>
        /// <param name="Descr">More details</param>
        public void Add(string Method, string Descr)
        {
            this.Add(new SQL_Journal_Item { MethodName = Method, Description = Descr });
        }
    }

    internal class Access
    {
        private bool locked = false;
        public int TimeOut { get; set; }

        public Access()
        {
            TimeOut = 1 * 60 * 1000; // minutes
        }

        public Access(int TimeOut)
        {
            this.TimeOut = TimeOut;
        }

        public void WaitForUnlocked()
        {
            DateTime waitStart = DateTime.Now;

            while (locked)
            {
                System.Threading.Thread.Sleep(16);
                TimeSpan waitDuration = DateTime.Now - waitStart;
                if (waitDuration.TotalMilliseconds >= TimeOut)
                {
                    throw (new Exception("Access.WaitForUnlocked time out!"));
                }
            }
        }

        public void Lock()
        {
            WaitForUnlocked();
            locked = true;
        }

        public void UnLock()
        {
            locked = false;
        }
    }

   

    /// <summary>
    /// Author: Nicolas Chourot (all rights reserved)
    /// This class hold the connection string of a data base and an optional SQL requests log journal.
    /// It also handle SQL transactions
    /// </summary>
    public class DataBase
    {
        /// <summary>
        /// The data base connection string
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string ConnectionString { get; }

        /// <summary>
        /// Set to true to keep all SQL requests in the SQL journal otherwise if the SQL_journal is not null with keep track of SQL request errors only
        /// </summary>
        public bool TrackSQL { get; set; }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public SQL_Journal SQL_Journal { get; set; }

        private readonly Dictionary<int, SqlTransaction> Transactions = new Dictionary<int, SqlTransaction>();

        [EditorBrowsable(EditorBrowsableState.Never)]
        public SqlTransaction Transaction
        {
            get
            {
                int _ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
                return Transactions[_ThreadId];
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Add_To_SQL_Journal(string Method, string Descr)
        {
            if (SQL_Journal != null)
                SQL_Journal.Add("[" + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() + "] " + Method, Descr);
        }

        ///<summary>
        ///Constructor with connectionString
        ///<para>Sql Express V12</para>
        ///<para>String BDFI = @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename='" + DB_Path + "'; Integrated Security=true;Max Pool Size=1024;Pooling=true;";</para>
        ///<para>Sql Express V11</para>
        ///<para>String BDFI = @"Data Source=(LocalDB)\v11.0;AttachDbFilename='" + DB_Path + "'; Integrated Security=true;Max Pool Size=1024;Pooling=true;";</para>
        ///</summary>
        ///<param name="ConnectionString">The data base connection string</param>
        ///<param name="SQL_Journal_FilePath">Optinal parameter for the SQL journal file path</param>
        public DataBase(string ConnectionString, string SQL_Journal_FilePath = "")
        {
            this.ConnectionString = ConnectionString;
            TrackSQL = false;
            if (!string.IsNullOrEmpty(SQL_Journal_FilePath))
            {
                SQL_Journal = new SQL_Journal(SQL_Journal_FilePath);
            }
            else
            {
                SQL_Journal = null;
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TransactionRunning()
        {           
            return Transactions.ContainsKey(System.Threading.Thread.CurrentThread.ManagedThreadId);
        }

        private void RemoveTransaction()
        {
            if (TransactionRunning())
            {
                if (Transaction.Connection != null)
                {
                    Transaction.Connection.Close();
                    Transaction.Connection.Dispose();
                }
                Transaction.Dispose();
                Transactions.Remove(System.Threading.Thread.CurrentThread.ManagedThreadId);
            }
        }

        /// <summary>
        /// Begin a SQL transaction
        ///<para/>Example
        ///<para/>bool error = false;
        ///<para/>try
        ///<para/>{
        ///<para/>   dataBaseObj.BeginTransaction("Optional Description");
        ///<para/>   // do some sql requests
        ///<para/>}
        ///<para/>catch(Exception)
        ///<para/>{
        ///<para/>   error = true;
        ///<para/>}
        ///<para/>finally
        ///<para/>{
        ///<para/>   dataBaseObj.EndTransaction(error); // if (error) Rollback... else Commit...
        ///<para/>}
        /// </summary>
        public void BeginTransaction(string name = "")
        {
            bool showName = (!string.IsNullOrEmpty(name));
            int _ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
           
            if (name.Length > 32)
                name = name.Substring(28) + "...";
            if (TransactionRunning())
            {
                Add_To_SQL_Journal("DAL.DataBase.BeginTransaction Error!", "A transaction is currently running, can't start another one! " + (showName? name: ""));
                throw new Exception("DAL.DataBase.BeginTransaction Error! A transaction is currently running, can't start another one! ");
            }
            else
            {
                try
                {
                    SqlConnection Connection = new SqlConnection(ConnectionString);
                    Connection.Open();
                    Transactions.Add(_ThreadId, Connection.BeginTransaction(name));
                    if (TrackSQL) Add_To_SQL_Journal("DAL.DataBase.BeginTransaction. ", (showName ? name : ""));
                }
                catch (Exception ex)
                {
                    Add_To_SQL_Journal("DAL.DataBase.BeginTransaction Error!", (showName ? name : ""));
                    Add_To_SQL_Journal(ex.Message, "");
                    RemoveTransaction();
                }
            }
        }
        /// <summary>
        /// End an SQL transaction. Usage :
        /// dataBaseObj.EndTransaction(error); // if (error) Rollback... else Commit...
        /// </summary>
        /// <param name="error"></param>
        public void EndTransaction(bool error)
        {
            int _ThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
            if (!TransactionRunning())
            {
                Add_To_SQL_Journal("DAL.DataBase.EndTransaction Error!", "No transaction was started, can't commit or rollback. ");
                throw new Exception("DAL.DataBase.EndTransaction Error! No transaction was started, can't commit or rollback.");
            }
            if (!error)
            {                
                try
                {
                    Transaction.Commit();
                    if (TrackSQL) Add_To_SQL_Journal("DAL.DataBase.EndTransaction Commit ", "");
                }
                catch (Exception ex)
                {
                    Add_To_SQL_Journal("DAL.DataBase.EndTransaction Commit error!", ex.Message);
                    try
                    {
                        Transaction.Rollback();
                    }
                    catch (Exception ex2)
                    {
                        Add_To_SQL_Journal("DAL.DataBase.EndTransaction Rollback error!", ex2.Message);
                    }
                }
                finally
                {
                    RemoveTransaction();
                }
            }
            else
            {               
                try
                {
                    Transactions[_ThreadId].Rollback();
                    if (TrackSQL) Add_To_SQL_Journal("DAL.DataBase.EndTransaction Rollback ", "");
                }
                catch (Exception ex2)
                {
                    Add_To_SQL_Journal("DAL.DataBase.EndTransaction Rollback error!", ex2.Message);
                }
                finally
                {
                    RemoveTransaction();
                }
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // RecordsDB version beta
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Auteur : Nicolas Chourot (all rights reserved)
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    ///<summary>
    ///Author: Nicolas Chourot (all rights reserved)
    ///This class is dedicated to give access to CRUD queries on a specific target table of a database.
    ///<para>It assumed that the first field of the target table if named "Id" of type int IDENTITY(1,1)</para>
    ///<para>The derived user class should be named with target table name.</para>
    ///<para>The provided type RecordType must be another user class which match the target table record fields:</para>
    ///<para>  public class Record</para>
    ///<para>  {</para>
    ///<para>      public int Id {get; set;}  IMPORTANT: the first member has to be of type int and named Id</para>
    ///<para>      public Type Field_name_1 {get; set;}</para>
    ///<para>      public Type Field_name_2 {get; set;}</para>
    ///<para>      ...</para>
    ///<para>      public Type Field_name_n {get; set;}</para>
    ///<para></para>
    ///<para>      public Record() {} // default constructor</para>
    ///<para>  }</para>
    ///<para></para>
    ///</summary>
    ///<typeparam name="RecordType"></typeparam>
    public class RecordsDB<RecordType>
    {
        #region Public functions & properties
        public DateTime LastUpdate { get; set; }
        public DataBase DataBase { get; set; }
        private readonly Asset<RecordType> Asset = new Asset<RecordType>();
        
        private string _SerialNumber;

        public void MarkHasChanged()
        {
            _SerialNumber = Guid.NewGuid().ToString();
        }
        public bool HasChanged
        {
            get
            {
                string key = this.GetType().Name;
                if (HttpContext.Current.Session[key] != null && (string)HttpContext.Current.Session[key] != _SerialNumber)
                {
                    HttpContext.Current.Session[key] = _SerialNumber;
                    return true;
                }
                return false;
            }
        }
        ///<summary>
        ///Constructor
        ///</summary>
        ///<param name="DataBase">DataBase object initialised with the data base connection string</param>
        public RecordsDB(DataBase DataBase)
        {
            this.DataBase = DataBase;
            SetRecordType(typeof(RecordType));
            RecordType_Properties = currentRecord.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
            SQLTableName = this.GetType().Name;
            LastUpdate = DateTime.MaxValue;
            LastCacheUpdate = DateTime.MinValue;
            SetCache(cache);
        }

        ///<summary>
        ///Override the defaut SQL query SELECT * FROM [derived user class name]
        ///<para>used to maintain the cache that hold the list of items of type RecordType.</para>
        ///</summary>
        ///<param name="SQL"></param>
        public void SetCacheSQL(String SQL = "")
        {
            if (SQL == "")
            {
                CacheSQL = "SELECT * FROM " + SQLTableName;
            }
            else
            {
                CacheSQL = SQL;
            }
        }

        ///<summary>
        ///Specify the used of a cache or not
        ///</summary>
        ///<param name="activate">If true, activate the use of a cache that hold a list of items of type RecordType</param>
        ///<param name="SQL">Override the defaut SQL query SELECT * FROM [derived user class name]</param>
        public void SetCache(bool activate, String SQL = "")
        {
            cache = activate;
            if (cache)
            {
                SetCacheSQL(SQL);
            }
            else
            {
                ClearCache();
            }
        }

        ///<summary>
        ///Returns a list of items of type RecordType from the last query to database.
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="orderBy">Optional sql order by clause</param>
        ///<returns></returns>
        public Dictionary<int, RecordType> ToDictionary(string orderBy = "")
        {
            if (cache)
                return GetCache();

            string SQL = "SELECT * FROM " + SQLTableName;
            if (orderBy != "")
                SQL += " ORDER BY " + orderBy;

            return QuerySQL(SQL);
        }

        ///<summary>
        ///Returns a list of items of type RecordType from the last query to database.
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="orderBy">Optional sql order by clause</param>
        ///<returns></returns>
        public List<RecordType> ToList(string orderBy = "")
        {
            if (cache)
                return GetCache().Select(d => d.Value).ToList();

            string SQL = "SELECT * FROM " + SQLTableName;
            if (orderBy != "")
                SQL += " ORDER BY " + orderBy;

            return QuerySQL(SQL).Select(d => d.Value).ToList();
        }

        ///<summary>
        ///Return a list of items from of type RecordType from the SQL query to the database.
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="SQL">SQL query</param>
        ///<returns>List of objects of type RecordType.</returns>
        public List<RecordType> GetQuerySQL(string SQL)
        {
            return QuerySQL(SQL).Select(d => d.Value).ToList();
        }

        ///<summary>
        ///Return a item of type RecordType that correspond to the target table record of id = Id.
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="ID">the desired item of type RecordType where id = Id</param>
        ///<returns>Object of type RecordType.</returns>
        public virtual RecordType Get(int ID)
        {
            if (cache)
                return GetCacheItem(ID);

            Object record = null;
            string SQL = "SELECT * FROM " + SQLTableName + " WHERE Id = " + ID;
            Dictionary<int, RecordType> querySql = QuerySQL(SQL);
            if ((querySql.Count == 0))
                return (RecordType)record;

            return querySql[ID];
        }

        /// <summary>
        /// Return a list of items of type RecordType where FieldName = value
        /// </summary>
        /// <param name="FieldName">Target field name</param>
        /// <param name="value">Value to be matched</param>
        /// <returns></returns>
        public List<RecordType> GetByFieldName(String FieldName, object value)
        {
            if (cache)
                return GetCacheByFieldName(FieldName, value);

            string SQL = "SELECT * FROM " + SQLTableName + " WHERE " + FieldName + " = " + SQLHelper.ConvertValueFromMemberToSQL(value);

            return QuerySQL(SQL).Select(d => d.Value).ToList();
        }

        /// <summary>
        /// Get the first record of type RecordType where FieldName = value
        /// </summary>
        /// <param name="FieldName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public RecordType GetFirstByFieldName(string FieldName, object value)
        {
            if ((value != null) && (FieldNameExist(FieldName)))
            {
                List<RecordType> records = GetByFieldName(FieldName, value);
                if (records.Count > 0)
                    return records[0];
            }
            return default(RecordType);
        }

        ///<summary>
        ///Return a list of items of type RecordType that correspond to the target table records where 
        ///<para>DateTimeFieldName &gt;= min and DateTimeFieldName &lt;= max </para>
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="DateTimeFieldName"></param>
        ///<param name="min"></param>
        ///<param name="max"></param>
        ///<returns>list of objects of type RecordType.</returns>
        public List<RecordType> GetByPeriod(String DateTimeFieldName, DateTime min, DateTime max)
        {
            if (cache)
                return GetCacheByPeriod(DateTimeFieldName, min, max);

            String start = SQLHelper.DateSQLFormat((DateTime)min);
            String End = SQLHelper.DateSQLFormat((DateTime)max);
            string SQL = "SELECT * FROM " + SQLTableName + " WHERE " + DateTimeFieldName + " >= '" + start + "' AND " + DateTimeFieldName + " <= '" + End + "'";

            return QuerySQL(SQL).Select(d => d.Value).ToList();
        }

        ///<summary>
        ///Add a new record in the target table using the data from record of type RecordType.
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<param name="record">item of type RecordType.</param>
        ///<returns>the Id of the newly added record.</returns>
        public virtual int Add(RecordType record)
        {
            if (record != null)
            {
                MarkHasChanged();
                Asset.Update(record);
                SetRecordType(record.GetType());
                string SQL = "INSERT INTO " + SQLTableName + "(";

                // Check all members of type RecordType
                int i;
                for (i = 0; i < RecordType_Properties.Length; i++)
                {
                    if (RecordType_Properties[i].Name != "Id")
                    {
                        if (RecordType_Properties[i].Name[0] != '_') // exclude member with name that start with _
                        {
                            // check if member i is an attribute not a function
                            if (RecordType_Properties[i].GetIndexParameters().GetLength(0) == 0)
                            {
                                SQL += RecordType_Properties[i].Name + ", ";
                            }
                        }
                    }
                }

                SQL = SQL.Remove(SQL.LastIndexOf(", "), 2);
                SQL += ") VALUES (";

                // Check all members of type RecordType
                for (i = 0; i < RecordType_Properties.Length; i++)
                {
                    if (RecordType_Properties[i].Name != "Id")
                    {
                        if (RecordType_Properties[i].Name[0] != '_') // exclude member with name that start with _
                        {
                            // check if member i is an attribute not a function
                            if (RecordType_Properties[i].GetIndexParameters().GetLength(0) == 0)
                            {
                                SQL += SQLHelper.ConvertValueFromMemberToSQL(RecordType_Properties[i].GetValue(record, null)) + ", ";
                            }
                        }
                    }
                }
                SQL = SQL.Remove(SQL.LastIndexOf(", "), 2);
                SQL += ")";
                int result = NonQuerySQL(SQL);

                if (result == 0)
                    throw new Exception("DAL.RecordsDB.Add - No record affected!");
                GetLast();
                return GetCurrentRecordId();
            }
            else
                throw new Exception("DAL.RecordsDB.Add - null reference!");
        }

        ///<summary>
        ///Update a record in the target table using the data from record of type RecordType.
        ///</summary>
        ///<param name="record">Object of type RecordType.</param>
        public virtual int Update(RecordType record)
        {
            if (record != null)
            {
                MarkHasChanged();
                Asset.Update(record);
                SetRecordType(record.GetType());
                String SQL = "UPDATE " + SQLTableName + " ";
                SQL += "SET ";
                // parcourrir la liste après le champ Id
                for (int i = 0; i < RecordType_Properties.Length; i++)
                {
                    if (RecordType_Properties[i].Name != "Id")
                    {
                        if (RecordType_Properties[i].Name[0] != '_') // exclude member with name that start with _
                        {
                            if (RecordType_Properties[i].GetIndexParameters().GetLength(0) == 0)
                            {
                                SQL += "[" + RecordType_Properties[i].Name + "] = " + SQLHelper.ConvertValueFromMemberToSQL(RecordType_Properties[i].GetValue(record, null)) + ", ";
                            }
                        }
                    }
                }
                SQL = SQL.Remove(SQL.LastIndexOf(", "), 2);
                // Id
                SQL += " WHERE [Id] = " + SQLHelper.ConvertValueFromMemberToSQL(GetRecordId((RecordType)record));

                int result = NonQuerySQL(SQL);
                if (result == 0)
                    throw new Exception("DAL.RecordsDB.Update - No record affected!");
                return result;
            }
            else
                throw new Exception("DAL.RecordsDB.Update - null reference!");
        }

        ///<summary>
        ///Delete a record from the target table where id = Id.
        ///<para>Override this method if you want to make extra delete treatments.</para>
        ///</summary>
        ///<param name="ID">The record Id to delete.</param>
        public virtual int Delete(int ID)
        {
            MarkHasChanged();
            Asset.Delete(Get(ID));
            String sql = "DELETE FROM " + SQLTableName + " WHERE Id = " + ID;
            int result = NonQuerySQL(sql);
            if (result == 0)
                throw new Exception("DAL.RecordsDB.Delete - No record affected!");
            return result;
        }

        ///<summary>
        ///Delete records from the target table where fieldName = value.
        ///</summary>
        ///<param name="fieldName">the target field name of target table</param>
        ///<param name="value">a value that must have the same type of the target field type.</param>
        public int DeleteByFieldName(String fieldName, object value)
        {
            String SQL = "DELETE FROM " + SQLTableName + " WHERE " + fieldName + "= " + SQLHelper.ConvertValueFromMemberToSQL(value);
            return NonQuerySQL(SQL);
        }

        #endregion

        #region Private functions & attributes

        private PropertyInfo[] RecordType_Properties = null;
        /// Provides the datetime of the last update of cache
        private bool cache = false;
        private String CacheSQL;
        private DateTime LastCacheUpdate = new DateTime(0);
        private Dictionary<int, RecordType> Cache = new Dictionary<int, RecordType>();
        #endregion

        #region Record & reader handling
        private object currentRecord = null;
        private Type _recordType = typeof(RecordType);
        private int GetRecordId(RecordType record)
        {
            return (int)GetFieldValue(record, "Id");
        }
        private void SetRecordId(int Id, RecordType record)
        {
            SetFieldValue(record, "Id", Id);
        }
        private object GetFieldValue(RecordType record, string name)
        {
            return record.GetType().GetProperty(name).GetValue(record, null);
        }
        private void SetFieldValue(RecordType record, string name, object value)
        {
            record.GetType().GetProperty(name).SetValue(record, value, null);
        }
        private bool FieldNameExist(string name)
        {
            return (Activator.CreateInstance(typeof(RecordType)).GetType().GetProperty(name) != null);
        }
        private PropertyInfo[] RecordAttributes()
        {
            return currentRecord.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        }
        private Type GetRecordType()
        {
            return _recordType;
        }
        private object GetCurrentRecord()
        {
            return currentRecord;
        }
        private void SetRecordType(Type type)
        {
            ClearCache();
            currentRecord = Activator.CreateInstance(type);
            _recordType = type;
        }
        private int GetCurrentRecordId()
        {
            // The first member has to be named Id and type of int
            return GetRecordId((RecordType)currentRecord);
        }
        private int ReaderColumnIndex(string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i) == columnName)
                {
                    return i;
                }
            }
            return -1;
        }
        private Object ReaderColumnValue(string columnName)
        {
            int columnIndex = ReaderColumnIndex(columnName);
            if (columnIndex > -1)
                return SQLHelper.ConvertValueFromSQLToMember(reader.GetValue(columnIndex));
            else
                return null;
        }
        private void GetReaderValues()
        {
            currentRecord = Activator.CreateInstance(currentRecord.GetType());
            // Parcourrir la liste des membres
            for (int i = 0; i < RecordType_Properties.Length; i++)
            {
                // vérifier que le membre d'index i est un attribut
                if (RecordType_Properties[i].GetIndexParameters().GetLength(0) == 0)
                {
                    String memberName = RecordType_Properties[i].Name;

                    // vérifier qu'il y a un champ de la table qui porte le même nom que l'attribut
                    // avant de lui attribuer sa valeur
                    if (ReaderColumnIndex(memberName) > -1)
                    {
                        var value = ReaderColumnValue(memberName);
                        String typeValue = value.GetType().ToString();
                        if (typeValue != "System.DBNull")
                            RecordType_Properties[i].SetValue(currentRecord, value, null);
                        else
                            RecordType_Properties[i].SetValue(currentRecord, null, null);
                    }
                }
            }
        }
        private bool NextRecord()
        {
            bool NotEndOfReader = false;

            if (NotEndOfReader = reader.Read())
            {
                GetReaderValues();
            }
            return NotEndOfReader;
        }
        private bool Next()
        {
            if (reader != null)
            {
                bool more = NextRecord();
                if (!more)
                    EndQuerySQL();
                return more;
            }
            return false;
        }
        private Dictionary<int, RecordType> RecordsList()
        {
            Dictionary<int, RecordType> recordsDictionary = new Dictionary<int, RecordType>();
            if (reader != null)
                do
                {
                    recordsDictionary.Add(GetCurrentRecordId(), (RecordType)currentRecord);
                } while (Next());
            LastUpdate = DateTime.Now;
            return recordsDictionary;
        }
        private void ClearCache()
        {
            LastCacheUpdate = new DateTime(0);
            Cache.Clear();
        }
        private void UpdateCache()
        {
            if (cache)
            {
                if (LastCacheUpdate < LastUpdate)
                {
                    ClearCache();
                    Cache = QuerySQL(CacheSQL);
                    LastCacheUpdate = DateTime.Now;
                }
            }
        }
        ///<summary>
        ///Return the list of items of type RecordType issued by the last SQL query stored in the cache.
        ///<para>This method is usefull only if the cache is activated.</para>
        ///<para>The type RecordType is provided at instanciation of derived RecordsDB class.</para>
        ///</summary>
        ///<returns></returns>
        private Dictionary<int, RecordType> GetCache()
        {
            UpdateCache();
            //if (DataBase.TrackSQL)
            //    DataBase.Add_To_SQL_Journal("DAL.GetCacheItems", SQLTableName);
            return Cache;
        }
        private RecordType GetCacheItem(int Id)
        {
            UpdateCache();
            RecordType record;
            Cache.TryGetValue(Id, out record);
            // if (DataBase.TrackSQL)
            //    DataBase.Add_To_SQL_Journal("DAL.GetCacheItem", SQLTableName + @"/Id = " + Id.ToString());
            return (RecordType)record;
        }
        private List<RecordType> GetCacheByFieldName(string name, object value)
        {
            UpdateCache();
            List<RecordType> records = new List<RecordType>();
            if ((value != null) && (FieldNameExist(name)))
            {
                foreach (RecordType record in Cache.Values)
                {
                    object fieldValue = GetFieldValue(record, name);
                    if (GetFieldValue(record, name).ToString() == value.ToString())
                        records.Add(record);
                }
            }
            return records;
        }
        private List<RecordType> GetCacheByPeriod(string name, DateTime min, DateTime max)
        {
            UpdateCache();
            List<RecordType> records = new List<RecordType>();
            if (FieldNameExist(name))
            {
                foreach (RecordType record in Cache.Values)
                {
                    DateTime fieldValue = (DateTime)GetFieldValue(record, name);
                    if ((DateTime.Compare(fieldValue, min) > 0) &&
                        (DateTime.Compare(fieldValue, max) < 0))
                        records.Add(record);
                }
            }
            return records;
        }
        #endregion

        #region Queries handling
        private SqlConnection connection = null;
        private SqlDataReader reader = null;
        private String SQLTableName = "";
        private Access access = new Access();
        bool localTransaction = false;
        private Dictionary<int, RecordType> GetLast()
        {
            string SQL = "SELECT TOP 1 * FROM " + SQLTableName + " ORDER BY ID DESC";
            return QuerySQL(SQL);
        }
        private Dictionary<int, RecordType> QuerySQL(string sqlCommand)
        {
           
            access.Lock();
            localTransaction = false;
            if (!DataBase.TransactionRunning())
            {
                DataBase.BeginTransaction();
                localTransaction = true;
            }

            try
            {
                if (DataBase.TrackSQL)
                {
                    DataBase.Add_To_SQL_Journal("DAL.RecordsDB.QuerySQL ", sqlCommand);
                }

                SqlCommand sqlcmd = DataBase.Transaction.Connection.CreateCommand();
                sqlcmd.Transaction = DataBase.Transaction;
                sqlcmd.Connection = DataBase.Transaction.Connection;
                sqlcmd.CommandText = sqlCommand;

                try
                {
                    reader = sqlcmd.ExecuteReader();
                    if (!reader.HasRows)
                        EndQuerySQL();
                }
                catch (Exception ex1)
                {
                    DataBase.Add_To_SQL_Journal("DAL.RecordsDB.QuerySQL - SQL Error!", " Exception message: " + ex1.Message);
                    DataBase.Add_To_SQL_Journal("", sqlCommand);
                    EndQuerySQL();
                }
            }
            catch (Exception ex2)
            {
                DataBase.Add_To_SQL_Journal("DAL.RecordsDB.QuerySQL - Connection Error!", " Exception message: " + ex2.Message);
                EndQuerySQL();
            }

            if (reader != null)
            {
                Next();
            }
            return RecordsList();
        }
        private void EndQuerySQL()
        {
            try
            {
                if (reader != null)
                {
                    reader.Close();
                    reader.Dispose();
                    reader = null;
                }
                if (connection != null)
                {
                    connection.Close();
                    connection.Dispose();
                    connection = null;
                }
            }
            catch (Exception ex)
            {
                DataBase.Add_To_SQL_Journal("DAL.RecordsDB.EndQuerySQL - SQL connection error!", " Exception message: " + ex.Message);
            }
            finally
            {
                access.UnLock();
                if (localTransaction)
                    DataBase.EndTransaction(false);
            }
        }
        /// <summary>
        /// Submit a sql command. Must be encapsulated within a transaction otherwise a local transaction will be applied.
        /// </summary>
        /// <param name="sqlCommandText"></param>
        public int NonQuerySQL(string sqlCommandText)
        {
            int AffectedRecords = 0;
            /* if (!DataBase.TransactionRunning())
            {
                DataBase.Add_To_SQL_Journal("DAL.RecordsDB.NonQuerySQL", " No transaction running!" + " " + sqlCommandText);
                throw new Exception("DAL.RecordsDB.NonQuerySQL - No transaction running!" + " " + sqlCommandText);
            }*/
 
            try
            {
                access.Lock();
                localTransaction = false;
                if (!DataBase.TransactionRunning())
                {
                    DataBase.Add_To_SQL_Journal("DAL.RecordsDB.NonQuerySQL", " WARNING! No transaction running!" + " " + sqlCommandText);
                    localTransaction = true;
                }
                if (localTransaction)
                    DataBase.BeginTransaction();
                if (DataBase.TrackSQL)
                {
                    DataBase.Add_To_SQL_Journal("DAL.RecordsDB.NonQuerySQL", sqlCommandText);
                }
                SqlCommand command = DataBase.Transaction.Connection.CreateCommand();
                command.Transaction = DataBase.Transaction;
                command.CommandText = sqlCommandText;
                try
                {
                    AffectedRecords = command.ExecuteNonQuery();
                }
                catch (Exception ex1)
                {
                    DataBase.Add_To_SQL_Journal("DAL.RecordsDB.NonQuerySQL - SQL error!", " Exception message: " + ex1.Message);
                    DataBase.Add_To_SQL_Journal("", sqlCommandText);
                }
            }
            catch (Exception ex3)
            {
                DataBase.Add_To_SQL_Journal("DAL.RecordsDB.NonQuerySQL - SQL connection error!", " Exception message: " + ex3.Message);
                DataBase.Add_To_SQL_Journal("", sqlCommandText);
            }
            finally
            {
                if (localTransaction)
                    DataBase.EndTransaction(false);
                LastUpdate = DateTime.Now;
                ClearCache();
                access.UnLock();
            }
            return AffectedRecords;
        }

        #endregion
    }
    internal class SQLHelper
    {
        static public string PrepareForSql(string text)
        {
            return text.Replace("'", "&c&");
        }

        static public string FromSql(string text)
        {
            return text.Replace("&c&", "'");
        }

        static string TwoDigit(int n)
        {
            string s = n.ToString();
            if (n < 10)
                s = "0" + s;
            return s;
        }

        public static string DateSQLFormat(DateTime date)
        {
            return date.Year + "-" + TwoDigit(date.Month) + "-" + TwoDigit(date.Day) + " " + TwoDigit(date.Hour) + ":" + TwoDigit(date.Minute) + ":" + TwoDigit(date.Second) + ".000";
        }

        public static bool IsNumericType(Type type)
        {
            if (type == null)
            {
                return false;
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
                case TypeCode.Object:
                    if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        return IsNumericType(Nullable.GetUnderlyingType(type));
                    }
                    return false;
            }
            return false;
        }

        public static object ConvertValueFromSQLToMember(Object memberValue)
        {
            if (memberValue.GetType() == typeof(String))
                return SQLHelper.FromSql(memberValue.ToString()).Trim();
            else
                return memberValue;
        }

        public static String ConvertValueFromMemberToSQL(Object memberValue)
        {
            String Sql_value = "";
            if (memberValue != null)
            {

                if (SQLHelper.IsNumericType(memberValue.GetType()))
                {
                    if (memberValue.GetType().IsEnum)
                        Sql_value = ((int)memberValue).ToString();
                    else
                        Sql_value = memberValue.ToString().Replace(',', '.');
                }
                else
                {
                    if (memberValue.GetType() == typeof(DateTime))
                        Sql_value = "'" + SQLHelper.DateSQLFormat((DateTime)memberValue) + "'";
                    else
                        if (memberValue.GetType() == typeof(System.Boolean))
                        Sql_value = ((System.Boolean)memberValue ? "1" : "0");
                    else
                        Sql_value = "'" + SQLHelper.PrepareForSql((String)memberValue) + "'";
                }
            }
            else
            {
                //Sql_value = " NULL ";
                Sql_value = " '' ";
            }
            return Sql_value;
        }
    }
    /// <summary>
    /// This class is for encryption and decryption of any string
    /// </summary>
    public static class Encryption
    {
        static int keyLength = 10;

        private static String GenerateKey()
        {
            String key = "";

            Random random = new Random(DateTime.Now.Millisecond);
            String charPool = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ{}[]()=+-_/*!'$%?&¦@#:;|0123456789";
            for (int i = 0; i < keyLength; i++)
            {
                key += charPool[random.Next(0, charPool.Length - 1)];
            }
            return key;
        }

        //http://www.codeproject.com/Articles/14151/Encrypt-and-Decrypt-Data-with-Csharp

        /// <summary>
        /// Encrypt a string
        /// </summary>
        /// <param name="toEncrypt"></param>
        /// <returns>encrypted string</returns>
        public static String Encrypt(string toEncrypt)
        {
            if (!String.IsNullOrEmpty(toEncrypt))
            {
                string key = GenerateKey();
                byte[] keyArray;
                byte[] toEncryptArray = UTF8Encoding.UTF8.GetBytes(toEncrypt);

                MD5CryptoServiceProvider hashmd5 = new MD5CryptoServiceProvider();
                keyArray = hashmd5.ComputeHash(UTF8Encoding.UTF8.GetBytes(key));
                //Always release the resources and flush data
                //of the Cryptographic service provide. Best Practice

                hashmd5.Clear();

                TripleDESCryptoServiceProvider tdes = new TripleDESCryptoServiceProvider();

                //set the secret key for the tripleDES algorithm
                tdes.Key = keyArray;
                //mode of operation. there are other 4 modes. We choose ECB(Electronic code Book)
                tdes.Mode = CipherMode.ECB;
                //padding mode(if any extra byte added)
                tdes.Padding = PaddingMode.PKCS7;

                ICryptoTransform cTransform = tdes.CreateEncryptor();
                //transform the specified region of bytes array to resultArray
                byte[] resultArray = cTransform.TransformFinalBlock
                        (toEncryptArray, 0, toEncryptArray.Length);
                //Release resources held by TripleDes Encryptor
                tdes.Clear();
                //Return the key and encrypted data into unreadable string format
                return key + Convert.ToBase64String(resultArray, 0, resultArray.Length);
            }
            return "";
        }
        /// <summary>
        /// Decrypt a string
        /// </summary>
        /// <param name="toDecrypt"></param>
        /// <returns>decrypted string</returns>
        public static String Decrypt(string toDecrypt)
        {
            if (!String.IsNullOrEmpty(toDecrypt))
            {
                // Extract the key stored in the first keyLength characters
                String key = toDecrypt.Substring(0, keyLength);
                // Remove the key from the toDecrypt string
                toDecrypt = toDecrypt.Substring(keyLength, toDecrypt.Length - keyLength);

                byte[] keyArray;
                //get the byte code of the string

                byte[] toEncryptArray = Convert.FromBase64String(toDecrypt);

                MD5CryptoServiceProvider hashmd5 = new MD5CryptoServiceProvider();
                keyArray = hashmd5.ComputeHash(UTF8Encoding.UTF8.GetBytes(key));
                //Always release the resources and flush data
                //of the Cryptographic service provide. Best Practice

                hashmd5.Clear();

                TripleDESCryptoServiceProvider tdes = new TripleDESCryptoServiceProvider();
                //set the secret key for the tripleDES algorithm
                tdes.Key = keyArray;
                //mode of operation. there are other 4 modes.
                //We choose ECB(Electronic code Book)

                tdes.Mode = CipherMode.ECB;
                //padding mode(if any extra byte added)
                tdes.Padding = PaddingMode.PKCS7;
                byte[] resultArray;
                try
                {
                    ICryptoTransform cTransform = tdes.CreateDecryptor();
                    resultArray = cTransform.TransformFinalBlock
                            (toEncryptArray, 0, toEncryptArray.Length);
                }
                catch (Exception)
                {
                    return "";
                }
                //Release resources held by TripleDes Encryptor
                tdes.Clear();
                //return the Clear decrypted TEXT
                return UTF8Encoding.UTF8.GetString(resultArray);
            }
            return "";
        }
    }

}
