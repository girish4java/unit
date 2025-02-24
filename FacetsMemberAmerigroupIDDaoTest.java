package com.elevancehealth.dckr.microsvc.aksgbdsoamembereligibility;


import com.amerigroup.facets.dao.FacetsMemberDaoImpl;
import com.amerigroup.facets.dao.dto.FacetsMemberAmerigroupIDDto;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.sql.Connection;
import java.sql.Date;
import java.util.List;
import static org.junit.Assert.*;


public class FacetsMemberAmerigroupIDDaoTest {
    private IDatabaseTester databaseTester;
    private FacetsMemberDaoImpl dao;


    @Before
    public void setUp() throws Exception {
        EnvironmentResolver env = new EnvironmentResolver();
        databaseTester = new JdbcDatabaseTester("org.h2.Driver", "jdbc:h2:mem:testdb", "sa", "");
        Connection connection = databaseTester.getConnection().getConnection();


        // Create the cmc_cspi_cs_plan table
        String createTableSQL1 = "CREATE TABLE cmc_cspi_cs_plan (" +
                "GRGR_CK VARCHAR(255), " +
                "CSCS_ID VARCHAR(255), " +
                "CSPI_ID VARCHAR(255), " +
                "CSPD_CAT VARCHAR(255), " +
                "CSPI_ITS_PREFIX VARCHAR(255), " +
                "CSPI_EFF_DT DATE, " +
                "CSPI_TERM_DT DATE)";
        connection.createStatement().execute(createTableSQL1);

        // Create the cmc_mepe_prcs_elig table with correct column names
        String createTableSQL2 = "CREATE TABLE cmc_mepe_prcs_elig (" +
                "MEPE_ID VARCHAR(255), " +
                "PRCS_ID VARCHAR(255), " +
                "ELIG_STATUS VARCHAR(255), " +
                "MEPE_EFF_DT DATE, " +
                "MEPE_TERM_DT DATE, " +
                "GRGR_CK VARCHAR(255), " +
                "CSCPI_ID VARCHAR(255), " +
                "MEME_CK VARCHAR(255))";
        connection.createStatement().execute(createTableSQL2);

        // Create the cmc_meme_member table
        String createTableSQL3 = "CREATE TABLE cmc_meme_member (" +
                "MEME_CK VARCHAR(255), " +
                "SBSB_CK VARCHAR(255))";
        connection.createStatement().execute(createTableSQL3);

        // Create the cmc_sbsb_subsc table
        String createTableSQL4 = "CREATE TABLE cmc_sbsb_subsc (" +
                "SBSB_CK VARCHAR(255), " +
                "SBSB_ID VARCHAR(255), " +
                "GRGR_CK VARCHAR(255))";
        connection.createStatement().execute(createTableSQL4);



        IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResourceAsStream("/test-dataset.xml"));
        DatabaseOperation.CLEAN_INSERT.execute(new DatabaseConnection(connection), dataSet);
        
        dao = new FacetsMemberDaoImpl(); // Assuming a default constructor exists
    }

    @After
    public void tearDown() throws Exception {
        databaseTester.onTearDown();
    }

    @Test
    public void testGetIdAndPrefixBySbsbIdDateAndPrefix() throws Exception {
        String subscriberID = "SUB123";
        String prefix = "ABC";
        Date searchStartDate = Date.valueOf("2024-02-01");
        Date searchEndDate = Date.valueOf("2024-12-31");

        List<FacetsMemberAmerigroupIDDto> result = dao.getIdAndPrefixBySbsbIdDateAndPrefix(subscriberID, prefix, searchStartDate, searchEndDate);
        
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals("SUB123", result.get(0).amerigroupID);
        assertEquals("GRP001", result.get(0).groupID);
    }
}