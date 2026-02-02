using DAL;
using System;
namespace Models
{
    
    public class Town
    {
        public int Id { get; set; }
        public string Name { get; set; }
       
    }

    public class Towns : DAL.RecordsDB<Town>
    {
        public Towns(DAL.DataBase dataBase) : base(dataBase) { }
    }

}