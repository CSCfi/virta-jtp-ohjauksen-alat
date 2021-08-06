using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace OhjauksenAlat
{
    class Program
    {
        static void Main(string[] args)
        {

            if (args.Length != 1)
            {
                Console.Write("Argumenttien maara on vaara.");
            }
            else
            {

                string server = args[0];

                string connectionString_ods_julkaisut = "Server=" + server + ";Database=julkaisut_ods;Trusted_Connection=true";
                string connectionString_mds_julkaisut = "Server=" + server + ";Database=julkaisut_mds;Trusted_Connection=true";
               

                /* Kaydaan lapi julkaisut_mds.koodi.JulkaisunOhjauksenalat -taulu ja 
                 * lasketaan kullekin ko. taulun julkaisuntunnukselle ohjauksen alojen
                 * prosenttiosuudet. */

                Tietokantaoperaatiot tietokantaoperaatiot = new Tietokantaoperaatiot();

                SqlConnection conn_hae_julkaisut = new SqlConnection(connectionString_mds_julkaisut);
                SqlDataReader reader_hae_julkaisut = tietokantaoperaatiot.hae_julkaisut(conn_hae_julkaisut);

               
                while (reader_hae_julkaisut.Read())
                {

                    string julkaisunTunnus = (string) reader_hae_julkaisut["JulkaisunTunnus"];

                    /* Tutkitaan loytyyko julkaisuntunnukselle tekijoita, joille on ilmotettu
                     * alayksikkokoodi. Taman perusteella ohjauksen alat lasketaan seuraavasti:
                     * 
                     * 1. ei loydy tekijoita, joille ilmoitettu alayksikkokoodit => ohjauksen alat lasketaan julkaisuille ilmoitettujen koodien perusteella
                     * 2. loytyy tekijoita, joille ilmoitettu alayksikkokoodit => ohjauksen alat lasketaan tekijoille ilmoitettujen koodien perusteella 
                     */

                    bool loytyy_tekijoita_joilla_alayksikkokoodi = tietokantaoperaatiot.loytyy_tekijoita_joilla_alayksikkokoodi(server, julkaisunTunnus);


                    /* Jos ei loydy tekijoita, joille on ilmoitettu alayksikkokoodi, 
                     * niin ohjauksen alat lasketaan julkaisulle ilmoitettujen koodien perusteella. */
                    if (loytyy_tekijoita_joilla_alayksikkokoodi == false)
                    {

                        // Lasketaan kuinka monta alayksikkokoodia julkaisulle on ilmoitettu
                        int alayksikoita_julkaisulla = tietokantaoperaatiot.alayksikoita_julkaisulla(server, julkaisunTunnus);

                        if (alayksikoita_julkaisulla > 0)
                        {

                            // Haetaan alayksikot julkaisun perusteella
                            SqlConnection conn_alayksikot_julkaisulle = new SqlConnection(connectionString_ods_julkaisut);
                            SqlDataReader reader_alayksikot_julkaisulle = tietokantaoperaatiot.hae_alayksikot_organisaatiolle(conn_alayksikot_julkaisulle, julkaisunTunnus, "julkaisu");

                            decimal kasvatusalat_1 = 0;
                            decimal taiteet_ja_kulttuurialat_2 = 0;
                            decimal humanistiset_alat_3 = 0;
                            decimal yhteiskunnalliset_alat_4 = 0;
                            decimal kauppa_hallinto_ja_oikeustieteet_5 = 0;
                            decimal luonnontieteet_6 = 0;
                            decimal tietojenkasittely_ja_tietoliikenne_7 = 0;
                            decimal tekniikan_alat_8 = 0;
                            decimal maa_ja_metsatalousalat_9 = 0;
                            decimal laaketieteet_10 = 0;
                            decimal terveys_ja_hyvinvointialat_11 = 0;
                            decimal palvelualat_12 = 0;


                            while (reader_alayksikot_julkaisulle.Read())
                            {

                                string organisaatioTunnus = (string)reader_alayksikot_julkaisulle["OrganisaatioTunnus"];
                                string julkaisu_yksikko = (string)reader_alayksikot_julkaisulle["julkaisu_yksikko"];

                                // Haetaan ohjauksen alojen osuudet
                                SqlConnection conn_ohjausten_alojen_suhteet = new SqlConnection(connectionString_mds_julkaisut);
                                SqlDataReader reader_ohjausten_alojen_suhteet = tietokantaoperaatiot.ohjauksen_alojen_suhteet(conn_ohjausten_alojen_suhteet, organisaatioTunnus, julkaisu_yksikko);

                                while (reader_ohjausten_alojen_suhteet.Read())
                                {

                                    decimal ala1 = (decimal)reader_ohjausten_alojen_suhteet["kasvatusalat_1"];
                                    decimal ala2 = (decimal)reader_ohjausten_alojen_suhteet["taiteet_ja_kulttuurialat_2"];
                                    decimal ala3 = (decimal)reader_ohjausten_alojen_suhteet["humanistiset_alat_3"];
                                    decimal ala4 = (decimal)reader_ohjausten_alojen_suhteet["yhteiskunnalliset_alat_4"];
                                    decimal ala5 = (decimal)reader_ohjausten_alojen_suhteet["kauppa_hallinto_ja_oikeustieteet_5"];
                                    decimal ala6 = (decimal)reader_ohjausten_alojen_suhteet["luonnontieteet_6"];
                                    decimal ala7 = (decimal)reader_ohjausten_alojen_suhteet["tietojenkasittely_ja_tietoliikenne_7"];
                                    decimal ala8 = (decimal)reader_ohjausten_alojen_suhteet["tekniikan_alat_8"];
                                    decimal ala9 = (decimal)reader_ohjausten_alojen_suhteet["maa_ja_metsatalousalat_9"];
                                    decimal ala10 = (decimal)reader_ohjausten_alojen_suhteet["laaketieteet_10"];
                                    decimal ala11 = (decimal)reader_ohjausten_alojen_suhteet["terveys_ja_hyvinvointialat_11"];
                                    decimal ala12 = (decimal)reader_ohjausten_alojen_suhteet["palvelualat_12"];

                                    kasvatusalat_1 = kasvatusalat_1 + ala1 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    taiteet_ja_kulttuurialat_2 = taiteet_ja_kulttuurialat_2 + ala2 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    humanistiset_alat_3 = humanistiset_alat_3 + ala3 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    yhteiskunnalliset_alat_4 = yhteiskunnalliset_alat_4 + ala4 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    kauppa_hallinto_ja_oikeustieteet_5 = kauppa_hallinto_ja_oikeustieteet_5 + ala5 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    luonnontieteet_6 = luonnontieteet_6 + ala6 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    tietojenkasittely_ja_tietoliikenne_7 = tietojenkasittely_ja_tietoliikenne_7 + ala7 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    tekniikan_alat_8 = tekniikan_alat_8 + ala8 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    maa_ja_metsatalousalat_9 = maa_ja_metsatalousalat_9 + ala9 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    laaketieteet_10 = laaketieteet_10 + ala10 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    terveys_ja_hyvinvointialat_11 = terveys_ja_hyvinvointialat_11 + ala11 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);
                                    palvelualat_12 = palvelualat_12 + ala12 * (Convert.ToDecimal(1) / alayksikoita_julkaisulla);

                                }


                                reader_ohjausten_alojen_suhteet.Close();
                                conn_ohjausten_alojen_suhteet.Close();

                                tietokantaoperaatiot.update_julkaisunOhjauksenalat(server, julkaisunTunnus, kasvatusalat_1, taiteet_ja_kulttuurialat_2, humanistiset_alat_3, yhteiskunnalliset_alat_4, kauppa_hallinto_ja_oikeustieteet_5, luonnontieteet_6, tietojenkasittely_ja_tietoliikenne_7, tekniikan_alat_8, maa_ja_metsatalousalat_9, laaketieteet_10, terveys_ja_hyvinvointialat_11, palvelualat_12);

                            }

                            reader_alayksikot_julkaisulle.Close();
                            conn_alayksikot_julkaisulle.Close();

                        }

                    }


                    /*  jos sen sijaan loydetaan tekijoita, joille on ilmoitettu alayksikkokoodit, 
                     * niin lasketaan ohjauksen alat naiden koodien perusteella */
                    else
                    {

                        // Lasketaan kuinka monta alayksikkokoodia julkaisulle on ilmoitettu
                        //int alayksikoita_julkaisulla = tietokantaoperaatiot.alayksikoita_julkaisulla(server, julkaisunTunnus);

                        // Julkaisun tekijoiden maara
                        int julkaisun_tekijoiden_maara = tietokantaoperaatiot.julkaisun_tekijoiden_maara(server, julkaisunTunnus);

                        // OrganisaatioTunnus
                        string organisaatioTunnus = tietokantaoperaatiot.hae_organisaatio_julkaisunTunnuksella(server, julkaisunTunnus);

                        decimal tekijan_paino = Convert.ToDecimal(1) / julkaisun_tekijoiden_maara;

                        // AlayksikkoPainotus_TMP on taulu, jossa pidetaan valiaikaisesti dataa
                        tietokantaoperaatiot.insert_into_AlayksikkoPainotusTMP(server, julkaisunTunnus);

                        // Kaydaan lapi AlayksikkoPainotusTMP -taulu
                        SqlConnection conn_alayksikko_TMP = new SqlConnection(connectionString_ods_julkaisut);
                        SqlDataReader reader_alayksikkoTMP = tietokantaoperaatiot.select_alayksikko_TMP(conn_alayksikko_TMP);

                        decimal kasvatusalat_1 = 0;
                        decimal taiteet_ja_kulttuurialat_2 = 0;
                        decimal humanistiset_alat_3 = 0;
                        decimal yhteiskunnalliset_alat_4 = 0;
                        decimal kauppa_hallinto_ja_oikeustieteet_5 = 0;
                        decimal luonnontieteet_6 = 0;
                        decimal tietojenkasittely_ja_tietoliikenne_7 = 0;
                        decimal tekniikan_alat_8 = 0;
                        decimal maa_ja_metsatalousalat_9 = 0;
                        decimal laaketieteet_10 = 0;
                        decimal terveys_ja_hyvinvointialat_11 = 0;
                        decimal palvelualat_12 = 0;

                        while (reader_alayksikkoTMP.Read())
                        {

                            // tama on se painotus, jota kaytetaan alayksikkokoodin painona. Alustetaan nollaksi.
                            decimal painotus = 0;

                            string alayksikko = (string) reader_alayksikkoTMP["Alayksikko"];

                            SqlConnection conn_select_nimet = new SqlConnection(connectionString_ods_julkaisut);
                            SqlDataReader reader_select_nimet = tietokantaoperaatiot.select_nimet(conn_select_nimet, julkaisunTunnus, alayksikko);

                            while (reader_select_nimet.Read())
                            {

                                string etunimet = reader_select_nimet["Etunimet"] == System.DBNull.Value ? null : (string) reader_select_nimet["Etunimet"];
                                string sukunimi = reader_select_nimet["Sukunimi"] == System.DBNull.Value ? null : (string) reader_select_nimet["Sukunimi"];

                                // Lasketaan montako rivia palautaa ODS_Tekijat -taulusta, ylla olevilla etu- ja sukunimilla tarkasteltavana olevalle julkaisuntunnukselle
                                int riveja_tekijat_taulussa = tietokantaoperaatiot.riveja_tekijat_taulussa(server, julkaisunTunnus, etunimet, sukunimi);

                                // tarkistetaan ettei jaeta nollalla
                                if (riveja_tekijat_taulussa != 0) {
                                    painotus = painotus + (tekijan_paino / riveja_tekijat_taulussa); 
                                }

       
                            }

                            reader_select_nimet.Close();
                            conn_select_nimet.Close();

                            // paivitetaan painotus Alayksikko_TMP-tauluun
                            tietokantaoperaatiot.update_alayksikko_TMP_painot(server, alayksikko, painotus);


                            // Haetaan ohjauksen alojen osuudet
                            SqlConnection conn_ohjausten_alojen_suhteet = new SqlConnection(connectionString_mds_julkaisut);
                            SqlDataReader reader_ohjausten_alojen_suhteet = tietokantaoperaatiot.ohjauksen_alojen_suhteet(conn_ohjausten_alojen_suhteet, organisaatioTunnus, alayksikko);

                            while (reader_ohjausten_alojen_suhteet.Read())
                            {

                                decimal ala1 = (decimal)reader_ohjausten_alojen_suhteet["kasvatusalat_1"];
                                decimal ala2 = (decimal)reader_ohjausten_alojen_suhteet["taiteet_ja_kulttuurialat_2"];
                                decimal ala3 = (decimal)reader_ohjausten_alojen_suhteet["humanistiset_alat_3"];
                                decimal ala4 = (decimal)reader_ohjausten_alojen_suhteet["yhteiskunnalliset_alat_4"];
                                decimal ala5 = (decimal)reader_ohjausten_alojen_suhteet["kauppa_hallinto_ja_oikeustieteet_5"];
                                decimal ala6 = (decimal)reader_ohjausten_alojen_suhteet["luonnontieteet_6"];
                                decimal ala7 = (decimal)reader_ohjausten_alojen_suhteet["tietojenkasittely_ja_tietoliikenne_7"];
                                decimal ala8 = (decimal)reader_ohjausten_alojen_suhteet["tekniikan_alat_8"];
                                decimal ala9 = (decimal)reader_ohjausten_alojen_suhteet["maa_ja_metsatalousalat_9"];
                                decimal ala10 = (decimal)reader_ohjausten_alojen_suhteet["laaketieteet_10"];
                                decimal ala11 = (decimal)reader_ohjausten_alojen_suhteet["terveys_ja_hyvinvointialat_11"];
                                decimal ala12 = (decimal)reader_ohjausten_alojen_suhteet["palvelualat_12"];

                                kasvatusalat_1 = kasvatusalat_1 + ala1 * painotus;
                                taiteet_ja_kulttuurialat_2 = taiteet_ja_kulttuurialat_2 + ala2 * painotus;
                                humanistiset_alat_3 = humanistiset_alat_3 + ala3 * painotus;
                                yhteiskunnalliset_alat_4 = yhteiskunnalliset_alat_4 + ala4 * painotus;
                                kauppa_hallinto_ja_oikeustieteet_5 = kauppa_hallinto_ja_oikeustieteet_5 + ala5 * painotus;
                                luonnontieteet_6 = luonnontieteet_6 + ala6 * painotus;
                                tietojenkasittely_ja_tietoliikenne_7 = tietojenkasittely_ja_tietoliikenne_7 + ala7 * painotus;
                                tekniikan_alat_8 = tekniikan_alat_8 + ala8 * painotus;
                                maa_ja_metsatalousalat_9 = maa_ja_metsatalousalat_9 + ala9 * painotus;
                                laaketieteet_10 = laaketieteet_10 + ala10 * painotus;
                                terveys_ja_hyvinvointialat_11 = terveys_ja_hyvinvointialat_11 + ala11 * painotus;
                                palvelualat_12 = palvelualat_12 + ala12 * painotus;

                            }


                            reader_ohjausten_alojen_suhteet.Close();
                            conn_ohjausten_alojen_suhteet.Close();

                            tietokantaoperaatiot.update_julkaisunOhjauksenalat(server, julkaisunTunnus, kasvatusalat_1, taiteet_ja_kulttuurialat_2, humanistiset_alat_3, yhteiskunnalliset_alat_4, kauppa_hallinto_ja_oikeustieteet_5, luonnontieteet_6, tietojenkasittely_ja_tietoliikenne_7, tekniikan_alat_8, maa_ja_metsatalousalat_9, laaketieteet_10, terveys_ja_hyvinvointialat_11, palvelualat_12);

                        }

                        reader_alayksikkoTMP.Close();
                        conn_alayksikko_TMP.Close();

                        // tyhjennetaan Alayksikko_TMP-taulu
                        tietokantaoperaatiot.delete_from_alayksikko_TMP(server);

                    }

                }

                reader_hae_julkaisut.Close();
                conn_hae_julkaisut.Close();

            }

            //Console.ReadLine(); 
             

        }

    }

}
