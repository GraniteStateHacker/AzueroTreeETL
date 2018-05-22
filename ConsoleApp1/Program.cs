using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var db = new MySqlConnection("Server=db.sundermedia.com; database=azuero_db; UID=azuero_db_user; password=STNHSTH; SslMode=None; Convert Zero DateTime=True"))
            {
                db.Open();
                using (var cmd = new MySqlCommand("select * from wp_posts where post_type='trees'", db))
                {
                    var reader = cmd.ExecuteReader();
                    var data = new DataSet();
                    var table = new DataTable("wp_posts");
                    table.Load(reader);
                    data.Tables.Add(table);
                    var ids = string.Join(",", (from aRow in table.AsEnumerable()
                                                select aRow.Field<int>(table.Columns["ID"])).ToArray());

                    using (var detailsCmd = new MySqlCommand(string.Format("select * from wp_postmeta where post_id in ({0})", ids), db))
                    {

                        var detailsReader = detailsCmd.ExecuteReader();
                        var detailsTable = new DataTable("wp_postmeta");
                        detailsTable.Load(detailsReader);
                        data.Tables.Add(detailsTable);

                    }
                    data.AcceptChanges();
                    using (var targetCmd = new MySqlCommand("select * from plants", db))
                    {
                        var targetTable = new DataTable("plants");
                        data.Tables.Add(targetTable);
                        targetTable.Load(targetCmd.ExecuteReader());
                        targetTable.AcceptChanges();
                        foreach (DataRow aPlant in data.Tables["wp_posts"].Rows)
                        {
                            int key = (int)aPlant["ID"];
                            DataRow targetPlant = targetTable.Rows.Find(key) ?? targetTable.NewRow();

                            targetPlant["plant_id"] = aPlant["ID"];
                            targetPlant["name"] = aPlant["post_name"];
                            targetPlant["slug"] = aPlant["post_title"];
                            targetPlant["language"] = aPlant["post_name"].ToString().EndsWith("-2") ? "es" : "en";
                            targetPlant["description"] = aPlant["post_content"];
                            targetPlant["height"] = string.Empty;
                            targetPlant["family"] = string.Empty;
                            targetPlant["type_id"] = 1;


                            var details = data.Tables["wp_postmeta"].Select("post_id = " + aPlant["ID"].ToString());
                            foreach (DataRow aDetail in details)
                            {
                                var value = aDetail["meta_value"];

                                switch (aDetail["meta_key"])
                                {
                                    case "height":
                                        targetPlant["height"] = value?.ToString().Length > 6 ? string.Empty : value;
                                        break;
                                    case "family":
                                        targetPlant["family"] = value;
                                        break;
                                    default:
                                        break;
                                }
                            }
                            targetTable.Rows.Add(targetPlant);
                            MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter("SELECT * FROM plants", db);
                            MySqlCommandBuilder cmdBuilder = new MySqlCommandBuilder(mySqlDataAdapter);
                            cmdBuilder.DataAdapter.Update(targetTable);
                            
                        }
                       
                    }
                }
            }
            Console.ReadKey();

          
        }
    }
}
