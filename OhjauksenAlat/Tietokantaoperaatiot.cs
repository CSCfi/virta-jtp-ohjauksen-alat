using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace OhjauksenAlat
{
    class Tietokantaoperaatiot
    {


        public SqlDataReader hae_julkaisut(SqlConnection conn)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT JulkaisunTunnus FROM koodi.JulkaisunOhjauksenalat";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }



        public bool loytyy_tekijoita_joilla_alayksikkokoodi(string server, string julkTunnus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT COUNT(DISTINCT tekija_yksikko) FROM dbo.JulkaisunTunnus_Alayksikko WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            if (maara > 0)
            {
                return true;
            }

            return false;

        }


        public int alayksikoita_julkaisulla(string server, string julkTunnus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT COUNT(DISTINCT julkaisu_yksikko) FROM dbo.JulkaisunTunnus_Alayksikko WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }



        public SqlDataReader hae_alayksikot_organisaatiolle(SqlConnection conn, string julkTunnus, string tyyppi)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "";

            if (tyyppi.Equals("julkaisu")) {
                query = "SELECT OrganisaatioTunnus, julkaisu_yksikko FROM dbo.JulkaisunTunnus_Alayksikko WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }
            else if (tyyppi.Equals("tekija"))
            {
                query = "SELECT OrganisaatioTunnus, tekija_yksikko FROM dbo.JulkaisunTunnus_Alayksikko WHERE JulkaisunTunnus = @JulkaisunTunnus";
            }

            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }




        public SqlDataReader ohjauksen_alojen_suhteet(SqlConnection conn, string orgTunnus, string alayksikko)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT kasvatusalat_1,taiteet_ja_kulttuurialat_2,humanistiset_alat_3,yhteiskunnalliset_alat_4,kauppa_hallinto_ja_oikeustieteet_5,luonnontieteet_6,tietojenkasittely_ja_tietoliikenne_7," +
                                    "tekniikan_alat_8,maa_ja_metsatalousalat_9,laaketieteet_10,terveys_ja_hyvinvointialat_11,palvelualat_12 " +
                                    "FROM koodi.OhjauksenalaYksikot " +
                                    "WHERE korkeakoulu_koodi = @korkeakoulu_koodi " +
                                    "AND alayksikko_koodi = @alayksikko_koodi";

            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(orgTunnus))
            {
                cmd.Parameters.AddWithValue("@korkeakoulu_koodi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@korkeakoulu_koodi", orgTunnus);
            }

            if (String.IsNullOrEmpty(alayksikko))
            {
                cmd.Parameters.AddWithValue("@alayksikko_koodi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@alayksikko_koodi", alayksikko);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        public void update_julkaisunOhjauksenalat(string server, string julkTunnus, decimal a1, decimal a2, decimal a3, decimal a4, decimal a5, decimal a6, decimal a7, decimal a8, decimal a9, decimal a10, decimal a11, decimal a12)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            using (conn)
            {

                SqlCommand cmd = new SqlCommand("UPDATE koodi.JulkaisunOhjauksenalat " +
                    "SET kasvatusalat_1 = @a1, taiteet_ja_kulttuurialat_2 = @a2, humanistiset_alat_3 = @a3, yhteiskunnalliset_alat_4 = @a4, kauppa_hallinto_ja_oikeustieteet_5 = @a5, " +
                    "luonnontieteet_6 = @a6, tietojenkasittely_ja_tietoliikenne_7 = @a7, tekniikan_alat_8 = @a8, maa_ja_metsatalousalat_9 = @a9, laaketieteet_10 = @a10, " +
                    "terveys_ja_hyvinvointialat_11 = @a11, palvelualat_12 = @a12 " +
                    "WHERE JulkaisunTunnus = @JulkaisunTunnus");
                
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;

                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }

                cmd.Parameters.AddWithValue("@a1", a1);
                cmd.Parameters.AddWithValue("@a2", a2);
                cmd.Parameters.AddWithValue("@a3", a3);
                cmd.Parameters.AddWithValue("@a4", a4);
                cmd.Parameters.AddWithValue("@a5", a5);
                cmd.Parameters.AddWithValue("@a6", a6);
                cmd.Parameters.AddWithValue("@a7", a7);
                cmd.Parameters.AddWithValue("@a8", a8);
                cmd.Parameters.AddWithValue("@a9", a9);
                cmd.Parameters.AddWithValue("@a10", a10);
                cmd.Parameters.AddWithValue("@a11", a11);
                cmd.Parameters.AddWithValue("@a12", a12);

                cmd.ExecuteNonQuery();

            }

            conn.Close();

        }


        public int julkaisun_tekijoiden_maara(string server, string julkTunnus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT COUNT(DISTINCT CONCAT(Etunimet, '-', Sukunimi)) FROM dbo.ODS_Tekijat WHERE JulkaisunTunnus = @JulkaisunTunnus AND Yksikko IS NOT NULL";
            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }


        public void insert_into_AlayksikkoPainotusTMP(string server, string julkTunnus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            using (conn)
            {

                SqlCommand cmd = new SqlCommand("INSERT INTO dbo.AlayksikkoPainotusTMP (Alayksikko) " +
                            "SELECT DISTINCT Yksikko FROM julkaisut_ods.dbo.ODS_Tekijat " +
                            "WHERE JulkaisunTunnus = @JulkaisunTunnus AND Yksikko IS NOT NULL");

                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;

                if (String.IsNullOrEmpty(julkTunnus))
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
                }
  
                cmd.ExecuteNonQuery();

            }

            conn.Close();

        }


        public SqlDataReader select_alayksikko_TMP(SqlConnection conn)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT Alayksikko FROM dbo.AlayksikkoPainotusTMP";

            cmd.Connection = conn;
            cmd.CommandText = query;

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;


        }


        public SqlDataReader select_nimet(SqlConnection conn, string julkTunnus, string yksikko)
        {

            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT Etunimet, Sukunimi FROM dbo.ODS_Tekijat WHERE JulkaisunTunnus = @JulkaisunTunnus AND Yksikko = @Yksikko";

            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            if (String.IsNullOrEmpty(yksikko))
            {
                cmd.Parameters.AddWithValue("@Yksikko", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Yksikko", yksikko);
            }

            SqlDataReader reader = cmd.ExecuteReader();

            return reader;

        }


        public int riveja_tekijat_taulussa(string server, string julkTunnus, string etunimet, string sukunimi)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            string query = "SELECT COUNT(*) FROM dbo.ODS_Tekijat WHERE JulkaisunTunnus = @JulkaisunTunnus AND Etunimet = @Etunimet AND Sukunimi = @Sukunimi AND Yksikko IS NOT NULL";
            
            if ((etunimet == null) && (sukunimi == null)) {
                conn.Close();
                return 0;
            }
            else if ((etunimet == null) && (sukunimi != null))
            {
                query = "SELECT COUNT(*) FROM dbo.ODS_Tekijat WHERE JulkaisunTunnus = @JulkaisunTunnus AND Etunimet IS NULL AND Sukunimi = @Sukunimi AND Yksikko IS NOT NULL";
            }
            

            cmd.Connection = conn;
            cmd.CommandText = query;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            if (String.IsNullOrEmpty(etunimet))
            {
                cmd.Parameters.AddWithValue("@Etunimet", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Etunimet", etunimet);
            }

            if (String.IsNullOrEmpty(sukunimi))
            {
                cmd.Parameters.AddWithValue("@Sukunimi", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@Sukunimi", sukunimi);
            }

            int maara = (int)cmd.ExecuteScalar();

            conn.Close();

            return maara;

        }


        public void update_alayksikko_TMP_painot(string server, string alayksikko, decimal painotus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            using (conn)
            {

                SqlCommand cmd = new SqlCommand("UPDATE dbo.AlayksikkoPainotusTMP SET Painotus = @Painotus WHERE Alayksikko = @Alayksikko");

                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;

                if (String.IsNullOrEmpty(alayksikko))
                {
                    cmd.Parameters.AddWithValue("@Alayksikko", DBNull.Value);
                }
                else
                {
                    cmd.Parameters.AddWithValue("@Alayksikko", alayksikko);
                }

                cmd.Parameters.AddWithValue("@Painotus", painotus);
                
                cmd.ExecuteNonQuery();

            }

            conn.Close();

        }


        public void delete_from_alayksikko_TMP(string server)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "DELETE FROM dbo.AlayksikkoPainotusTMP";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            cmd.ExecuteNonQuery();

            conn.Close();

        }


        public string hae_organisaatio_julkaisunTunnuksella(string server, string julkTunnus)
        {

            string connectionString = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
            SqlConnection conn = new SqlConnection(connectionString);
            conn.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = "SELECT TOP 1 OrganisaatioTunnus FROM dbo.JulkaisunTunnus_Alayksikko WHERE JulkaisunTunnus = @JulkaisunTunnus";
            cmd.CommandType = CommandType.Text;
            cmd.Connection = conn;

            if (String.IsNullOrEmpty(julkTunnus))
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", DBNull.Value);
            }
            else
            {
                cmd.Parameters.AddWithValue("@JulkaisunTunnus", julkTunnus);
            }

            string orgTunnus = cmd.ExecuteScalar().ToString();

            conn.Close();

            return orgTunnus;

        }

    }

}
