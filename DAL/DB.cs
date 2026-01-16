using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Web;
using static System.Collections.Specialized.BitVector32;
using Models;

namespace DAL
{
    public sealed class DB
    {
        private static readonly DB instance = new DB();
        private DB() {
            
        }
        public static DataBase DataBase { get; set; }
        public static Contacts Contacts{ get; set; }

        public static void Initialize(string DB_Path,
                                      string SQL_Journal_Path = "")
        {
            String MainDB_Connection_String = @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename='" + DB_Path + "'; Integrated Security=true;Max Pool Size=1024;Pooling=true;";
            DataBase = new DataBase(MainDB_Connection_String, SQL_Journal_Path)
            {
                TrackSQL = false
            };

            /* Declare tables access here */
            Contacts = new Contacts(DataBase);
        }
    }

}