using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Hosting;

namespace DAL
{
	sealed public class AssetAttribute : Attribute
	{
		private readonly string folder;
		private readonly string defaultValue;

		public AssetAttribute(string folder, string defaultValue)
		{
			this.folder = folder;
			this.defaultValue = defaultValue;
		}

		public string Folder() => folder;
		public string DefaultValue() => defaultValue;
	}

	public class Asset<T>
	{
		private object GetAttributeValue(T data, string attributeName)
		{
			return data.GetType().GetProperty(attributeName).GetValue(data, null);
		}
		// affecter la valeur de l'attribut attributeName de l'intance data de classe T
		private void SetAttributeValue(T data, string attributeName, object value)
		{
			data.GetType().GetProperty(attributeName).SetValue(data, value, null);
		}
		private bool IsBase64Value(string value)
		{
			bool isBase64 = value.Contains("data:") && value.Contains(";base64,");
			return isBase64;
		}
		public void Delete(T data)
		{
			var type = data.GetType();

			foreach (var property in type.GetProperties())
			{
				var attribute = property.GetCustomAttribute(typeof(AssetAttribute));

				if (attribute != null)
				{
					string assetsFolder = ((AssetAttribute)attribute).Folder();
					string defaultValue = ((AssetAttribute)attribute).DefaultValue();
					string propName = property.Name;
					string value = GetAttributeValue(data, propName).ToString();
					if (value != null && value != assetsFolder + defaultValue)
						File.Delete(HostingEnvironment.MapPath(value).ToString());
				}
			}
		}
		public void Update(T data)
		{
			var type = data.GetType();
			foreach (var property in type.GetProperties())
			{
				AssetAttribute assetAttribute = (AssetAttribute)property.GetCustomAttribute(typeof(AssetAttribute), true);

				if (assetAttribute != null)
				{
					string propName = property.Name;
					string propValue = GetAttributeValue(data, propName).ToString() ?? "";
					if (IsBase64Value(propValue)) // new asset
					{
						string assetsFolder = assetAttribute.Folder();
						string defaultValue = assetAttribute.DefaultValue();
						string previousAssetURL = propValue.Split('|')[0];
						string imagePart = propValue.Split('|')[1];
						if (previousAssetURL != "" && previousAssetURL != assetsFolder + defaultValue)
							File.Delete(HostingEnvironment.MapPath(previousAssetURL));
						string[] base64Data = imagePart.Split(',');
						string extension = base64Data[0].Replace(";base64", "").Split('/')[1];
						// IIS mime patch : does not serve webp and avif mimes
						if (extension.ToLower() == "webp") extension = "png";
						if (extension.ToLower() == "avif") extension = "png";
                        if (extension.ToLower() == "svg+xml") extension = "svg";
                        string assetData = base64Data[1];
						string assetUrl;
						string newAssetServerPath;
						do
						{
							var key = Guid.NewGuid().ToString();
							assetUrl = assetsFolder + key + "." + extension;
							newAssetServerPath = HostingEnvironment.MapPath(assetUrl);
							// make sure new file does not already exists 
						} while (File.Exists(newAssetServerPath));
						SetAttributeValue(data, propName, assetUrl);
						using (var stream = new MemoryStream(Convert.FromBase64String(assetData)))
						{
							using (var file = new FileStream(newAssetServerPath, FileMode.Create, FileAccess.Write))
								stream.WriteTo(file);
						}
					}
				}
			}
		}
	}
}