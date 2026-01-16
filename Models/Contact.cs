using DAL;
using System;
namespace Models
{
    
    public class Contact
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Phone { get; set; }
        public string Email { get; set; }

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