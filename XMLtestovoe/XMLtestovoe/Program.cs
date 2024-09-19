using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.Intrinsics.X86;
using System.Xml.Linq;
using Npgsql;
internal class Program
{
	private static void Main(string[] args)
	{

		string connString ="Server = localhost; Database = testovoe2; Port = 5432; Ssl Mode = allow; User Id = postgres; Password = ";

		var xml = XDocument.Load("g:\\myXML.xml");

		using (var conn = new NpgsqlConnection(connString))
		{
			conn.Open();
			var orders = xml.Element("orders");
			foreach (var order in orders.Elements("order"))
			{
				var no = order.Element("no").Value;
				var reg_date = order.Element("reg_date").Value;
				var sum = order.Element("sum").Value;
				var quantity = "";
				var name = "";
				var price = "";
				var fio = "";
				var email = "";

				foreach (var userData in order.Elements("user"))
				{
					fio = userData.Element("fio").Value;
					email = userData.Element("email").Value;
				}

				if (userExists(email))
				{
					using (var command = new NpgsqlCommand($@"UPDATE ""Пользователи"" SET ""имя_пользователя""='{fio}' WHERE email_пользователя='{email}'",conn))
					{
						var reader = command.ExecuteNonQuery();
					}
				}
				else
				{
					using (var command = new NpgsqlCommand($@"INSERT INTO ""Пользователи"" (""имя_пользователя"", ""email_пользователя"") VALUES ('{fio}','{email}')",conn))
					{
						var reader = command.ExecuteNonQuery();
					}
				}
				var userId = getUserId(email);
				if (recordExists(no))
				{
					using (var command = new NpgsqlCommand($@"UPDATE ""Покупки_товаров_пользователями"" SET ""пользователь_id""='{userId}', ""дата_заказа""='{reg_date}', ""цена_заказа""={sum} WHERE ""заказ_id""='{no}'",conn))
					{
						var reader = command.ExecuteNonQuery();
					}
				}
				else
				{
					using (var command = new NpgsqlCommand($@"INSERT INTO ""Покупки_товаров_пользователями"" (""заказ_id"",""пользователь_id"", ""дата_заказа"", ""цена_заказа"") VALUES ({no},{userId},'{reg_date}',{sum})",conn))
					{
						var reader = command.ExecuteNonQuery();
					}
				}

				using (var command = new NpgsqlCommand($@"DELETE FROM ""Корзина"" WHERE ""заказ_id""={no}",conn))
				{
					var reader = command.ExecuteNonQuery();
				}
				foreach (var productData in order.Elements("product"))
				{
					quantity = productData.Element("quantity").Value;
					name = productData.Element("name").Value;
					price = productData.Element("price").Value;

					if (productExists(name))
					{
						using (var command = new NpgsqlCommand($@"UPDATE ""Товары"" SET ""цена_товара""={price} WHERE ""название_товара""='{name}'",conn))
						{
							var reader = command.ExecuteNonQuery();
						}
					}
					else
					{
						using (var command = new NpgsqlCommand($@"INSERT INTO ""Товары"" (""название_товара"",""цена_товара"", ""производитель_id"", ""категория_id"") VALUES ('{name}',{price},'1','1')",conn))
						{
							var reader = command.ExecuteNonQuery();
						}
					}
					var productId = getProductId(name);
					using (var command = new NpgsqlCommand($@"INSERT INTO ""Корзина"" (""заказ_id"",""товар_id"",""количество_товара"") VALUES ({no},{productId},{quantity})",conn))
					{
						var reader = command.ExecuteNonQuery();
					}
				}
			}

			bool userExists(string email)
			{
				var check = "";
				using (var command = new NpgsqlCommand($@"SELECT * FROM ""Пользователи"" WHERE email_пользователя = '{email}'", conn))
				{
					var reader = command.ExecuteReader();
					while (reader.Read())
					{
						check = string.Format(reader.GetInt32(0).ToString());
					}
					reader.Close();
					if (check.Length > 0) return true;
					return false;
				}
			}
			bool recordExists(string no)
			{
				var check = "";
				using (var command = new NpgsqlCommand($@"SELECT * FROM ""Покупки_товаров_пользователями"" WHERE заказ_id = {no}", conn))
				{
					var reader = command.ExecuteReader();
					while (reader.Read())
					{
						check = string.Format(reader.GetInt32(0).ToString());
					}
					reader.Close();
					if (check.Length > 0) return true;
					return false;
				}
			}
			bool productExists(string name)
			{
				var check = "";
				using (var command = new NpgsqlCommand($@"SELECT * FROM ""Товары"" WHERE название_товара = '{name}'", conn))
				{
					var reader = command.ExecuteReader();
					while (reader.Read())
					{
						check = string.Format(reader.GetInt32(0).ToString());
					}
					reader.Close();
					if (check.Length > 0) return true;
					return false;
				}
			}
			string getUserId(string email) 
			{
				var userId = "";
				using (var command = new NpgsqlCommand($@"SELECT * FROM ""Пользователи"" WHERE email_пользователя = '{email}'", conn))
				{
					var reader = command.ExecuteReader();
					while (reader.Read())
					{
						userId = string.Format(reader.GetInt32(0).ToString());
					}
					reader.Close();
					return userId;
				}
			}
			string getProductId(string name)
			{
				var productId = "";
				using (var command = new NpgsqlCommand($@"SELECT * FROM ""Товары"" WHERE название_товара = '{name}'", conn))
				{
					var reader = command.ExecuteReader();
					while (reader.Read())
					{
						productId = string.Format(reader.GetInt32(0).ToString());
					}
					reader.Close();
					return productId;
				}
			}
		}
	}
}