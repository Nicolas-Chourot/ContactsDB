using Models;
using DAL;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Web.Mvc;

namespace ContactsDB.Controllers
{
    public class ContactsController : Controller
    {
       
        private void InitSessionVariables()
        {
            // Session is a dictionary that hold keys values specific to a session
            // Each user of this web application have their own Session
            // A Session has a default time out of 20 minutes, after time out it is cleared

            if (Session["CurrentContactId"] == null) Session["CurrentContactId"] = 0;
            if (Session["CurrentContactName"] == null) Session["CurrentContactName"] = "";
            if (Session["Search"] == null) Session["Search"] = false;
            if (Session["SearchString"] == null) Session["SearchString"] = "";
        }
        private void ResetCurrentContactInfo()
        {
            Session["CurrentContactId"] = 0;
            Session["CurrentContactName"] = "";
        }

        // This action produce a partial view of contacts
        // It is meant to be called by an AJAX request (from client script)
        public ActionResult GetContacts(bool forceRefresh = false)
        {
            IEnumerable<Contact> result = null;
            if (forceRefresh || DB.Contacts.HasChanged)
            {
                // forceRefresh is true when a related view is produce
                // DB.Contacts.HasChanged is true when a change has been applied on any contact

                InitSessionVariables();
                bool search = (bool)Session["Search"];
                string searchString = (string)Session["SearchString"];
                
                if (search)
                    result = DB.Contacts.ToList().Where(c => c.Name.ToLower().Contains(searchString)).OrderBy(c => c.Name);
                else
                    result = DB.Contacts.ToList().OrderBy(c => c.Name);
                return PartialView(result);
            }
            return null;
        }

        public ActionResult List()
        {
            ResetCurrentContactInfo();
            return View();
        }
        
        public ActionResult ToggleSearch()
        {
            if (Session["Search"] == null) Session["Search"] = false;
            Session["Search"] = !(bool)Session["Search"];
            return RedirectToAction("List");
        }
        
        public ActionResult SetSearchString(string value)
        {
            Session["SearchString"] = value.ToLower();
            return RedirectToAction("List");
        }

        public ActionResult About()
        {
            return View();
        }


        public ActionResult Details(int id)
        {
            Session["CurrentContactId"] = id;
            Contact contact = DB.Contacts.Get(id);
            if (contact != null)
            {
                Session["CurrentContactName"] = contact.Name;
                return View(contact);
            }
            return RedirectToAction("List");
        }
               public ActionResult Create()
        {
            return View(new Contact());
        }

        [HttpPost]
        /* Install anti forgery token verification attribute.
         * the goal is to prevent submission of data from a page 
         * that has not been produced by this application*/
        [ValidateAntiForgeryToken()] 
        public ActionResult Create(Contact contact)
        {
            DB.Contacts.Add(contact);
            return RedirectToAction("List");
        }

        public ActionResult Edit()
        {
            // Note that id is not provided has a parameter.
            // It use the Session["CurrentContactId"] set within
            // Details(int id) action
            // This way we prevent from malicious requests that could
            // modify or delete programatically the all the contacts

            int id = Session["CurrentContactId"] != null ? (int)Session["CurrentContactId"] : 0;
            if (id != 0)
            {
                Contact contact = DB.Contacts.Get(id);
                if (contact != null)
                    return View(contact);
            }
            return RedirectToAction("List");
        }
        
        [HttpPost]
        [ValidateAntiForgeryToken()]
        public ActionResult Edit(Contact contact)
        {
            // Has explained earlier, id of contact is stored server side an not provided in form data
            // passed in the method in order to prever from malicious requests

            int id = Session["CurrentContactId"] != null ? (int)Session["CurrentContactId"] : 0;

            // Make sure that the contact of id really exist
            Contact storedContact = DB.Contacts.Get(id);
            if (storedContact != null)
            {
                contact.Id = id; // patch the Id
                DB.Contacts.Update(contact);
            }
            return RedirectToAction("Details/" + id);
        }
        public ActionResult Delete()
        {
            int id = Session["CurrentContactId"] != null ? (int)Session["CurrentContactId"] : 0;
            if (id != 0)
            {
                DB.Contacts.Delete(id);
            }
            return RedirectToAction("List");
        }

        // This action is meant to be called by an AJAX request
        // Return true if there is a name conflict
        // Look into validation.js for more details
        // and also into Views/Contacts/ContactForm.cshtml
        public JsonResult CheckNameConflict(string Name)
        {
            int id = Session["CurrentContactId"] != null ? (int)Session["CurrentContactId"] : 0;
            // Response json value true if name is used in other contacts than the current contact
            return Json(DB.Contacts.ToList().Where(c => c.Name == Name && c.Id != id).Any(),
                        JsonRequestBehavior.AllowGet /* must have for CORS verification by client browser */);
        }

    }
}