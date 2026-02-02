using DAL;
using Newtonsoft.Json;
using System;
namespace Models
{
    
    public class Contact
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Phone { get; set; }
        public string Email { get; set; }
        public int TownId { get; set; } = 0;
        public DateTime Birth { get; set; } = DateTime.Now;

        public string _Town
        {
            get
            {
                if (TownId == 0)
                    return "Inconnue";
                else
                    return DB.Towns.Get(TownId).Name;
            }
        }

        public bool _IsBirthDay
        {
            get
            {   if (Birth != null)
                    return (Birth.Day == DateTime.Now.Day && Birth.Month == DateTime.Now.Month);
                else return false;
            }
        }

        const string Avatars_Folder = @"/App_Assets/Contacts/";
        const string Default_Avatar = @"no_avatar.png";
        [Asset(Avatars_Folder, Default_Avatar)]
        public string Avatar { get; set; } = Avatars_Folder + Default_Avatar;
    }

    public class Contacts : DAL.RecordsDB<Contact>
    {
        public Contacts(DAL.DataBase dataBase) : base(dataBase) { }
    }

}