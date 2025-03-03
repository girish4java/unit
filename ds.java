import com.github.database.rider.core.api.connection.ConnectionHolder;
import com.github.database.rider.core.api.dataset.DataSet;
import com.github.database.rider.junit5.DBUnitExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith({DBUnitExtension.class, MockitoExtension.class})
public class YourDaoClassTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @InjectMocks
    private YourDaoClass yourDaoClass; // Replace with the actual class name

    private ConnectionHolder connectionHolder = () -> connection;

    @BeforeEach
    public void setUp() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
    }

    @Test
    @DataSet("your-dataset.xml")
    public void testGetIdAndPrefixBySbsbIdDateAndPrefix() throws Exception {
        // Arrange
        String subscriberID = "12345";
        String prefix = "TEST";
        Date searchStartDate = new Date();
        Date searchEndDate = new Date();

        // Act
        List<FacetsMemberAmerigroupIDDto> result = yourDaoClass.getIdAndPrefixBySbsbIdDateAndPrefix(subscriberID, prefix, searchStartDate, searchEndDate);

        // Assert
        assertEquals(1, result.size());
        assertEquals("12345", result.get(0).amerigroupID);
        assertEquals("GRP123", result.get(0).groupID);
    }
}



<dataset>
    <cmc_sbsb_subsc SBSB_ID="12345" GRGR_CK="GRP123" sbsb_ck="1"/>
    <cmc_meme_member meme_ck="1" sbsb_ck="1"/>
    <cmc_mepe_prcs_elig meme_ck="1" GRGR_CK="GRP123" cscs_ID="1" cspi_ID="1" CSPD_CAT="CAT1" MEPE_EFF_DT="2023-01-01" MEPE_TERM_DT="2023-12-31"/>
    <CMC_CSPI_CS_PLAN GRGR_CK="GRP123" cscs_ID="1" cspi_ID="1" CSPD_CAT="CAT1" CSPI_ITS_PREFIX="TEST" CSPI_EFF_DT="2023-01-01" CSPI_TERM_DT="2023-12-31"/>
</dataset>



****************82


import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;

import javax.transaction.Transactional;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest // Bootstraps the Spring context
@ExtendWith(SpringExtension.class) // Integrates JUnit 5 with Spring
@TestExecutionListeners({
    DependencyInjectionTestExecutionListener.class,
    DirtiesContextTestExecutionListener.class,
    TransactionalTestExecutionListener.class,
    DbUnitTestExecutionListener.class // Enables DBUnit
})
@Transactional // Ensures the test runs in a transaction and rolls back after completion
public class YourDaoClassTest {

    @Autowired
    private YourDaoClass yourDaoClass; // Autowire your DAO class

    @Test
    @DatabaseSetup("classpath:your-dataset.xml") // Loads test data from this file
    public void testGetIdAndPrefixBySbsbIdDateAndPrefix() {
        // Arrange
        String subscriberID = "12345";
        String prefix = "TEST";
        Date searchStartDate = new Date();
        Date searchEndDate = new Date();

        // Act
        List<FacetsMemberAmerigroupIDDto> result = yourDaoClass.getIdAndPrefixBySbsbIdDateAndPrefix(subscriberID, prefix, searchStartDate, searchEndDate);

        // Assert
        assertEquals(1, result.size());
        assertEquals("12345", result.get(0).amerigroupID);
        assertEquals("GRP123", result.get(0).groupID);
    }
}



application-test.properties



# H2 Database Configuration
spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
spring.datasource.driver-class-name=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true





    *************************



    <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jdbc</artifactId>
</dependency>

<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>

<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
</dependency>

src/test/resources/application.properties
    spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.datasource.platform=h2
spring.sql.init.mode=always
spring.h2.console.enabled=true


    src/test/resources/schema.sql:
CREATE TABLE cmc_sbsb_subsc (
    SBSB_ID VARCHAR(50) PRIMARY KEY,
    GRGR_CK VARCHAR(50)
);

CREATE TABLE cmc_meme_member (
    meme_ck INT PRIMARY KEY,
    sbsb_ck VARCHAR(50),
    FOREIGN KEY (sbsb_ck) REFERENCES cmc_sbsb_subsc(SBSB_ID)
);

CREATE TABLE cmc_mepe_prcs_elig (
    mepe_ck INT PRIMARY KEY,
    meme_ck INT,
    GRGR_CK VARCHAR(50),
    cscs_ID VARCHAR(50),
    cspi_ID VARCHAR(50),
    CSPD_CAT VARCHAR(50),
    MEPE_EFF_DT DATE,
    MEPE_TERM_DT DATE,
    FOREIGN KEY (meme_ck) REFERENCES cmc_meme_member(meme_ck)
);

CREATE TABLE CMC_CSPI_CS_PLAN (
    GRGR_CK VARCHAR(50),
    cscs_ID VARCHAR(50),
    cspi_ID VARCHAR(50),
    CSPD_CAT VARCHAR(50),
    CSPI_ITS_PREFIX VARCHAR(50),
    CSPI_EFF_DT DATE,
    CSPI_TERM_DT DATE,
    PRIMARY KEY (GRGR_CK, cscs_ID, cspi_ID, CSPD_CAT)
);




src/test/resources/data.sql


    INSERT INTO cmc_sbsb_subsc (SBSB_ID, GRGR_CK) VALUES ('123456', 'G001');

INSERT INTO cmc_meme_member (meme_ck, sbsb_ck) VALUES (1, '123456');

INSERT INTO cmc_mepe_prcs_elig (mepe_ck, meme_ck, GRGR_CK, cscs_ID, cspi_ID, CSPD_CAT, MEPE_EFF_DT, MEPE_TERM_DT)
VALUES (1, 1, 'G001', 'CS001', 'CSP001', 'CAT001', '2024-01-01', '2024-12-31');

INSERT INTO CMC_CSPI_CS_PLAN (GRGR_CK, cscs_ID, cspi_ID, CSPD_CAT, CSPI_ITS_PREFIX, CSPI_EFF_DT, CSPI_TERM_DT)
VALUES ('G001', 'CS001', 'CSP001', 'CAT001', 'PRE001', '2024-01-01', '2024-12-31');



src/test/java/org/example/FacetsMemberDaoImplTest.java



    package org.example;

import org.example.dto.FacetsMemberAmerigroupIDDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@Import(FacetsMemberDaoImpl.class)
class FacetsMemberDaoImplTest {

    @Autowired
    private FacetsMemberDaoImpl facetsMemberDao;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void setupDatabase() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            assertNotNull(conn);
        }
    }

    @Test
    void testGetIdAndPrefixBySbsbIdDateAndPrefix() {
        String subscriberID = "123456";
        String prefix = "PRE001";
        Date searchStartDate = new Date(2024, 0, 1); // 2024-01-01
        Date searchEndDate = new Date(2024, 11, 31); // 2024-12-31

        List<FacetsMemberAmerigroupIDDto> result =
                facetsMemberDao.getIdAndPrefixBySbsbIdDateAndPrefix(subscriberID, prefix, searchStartDate, searchEndDate);

        assertNotNull(result);
        assertEquals(1, result.size());

        FacetsMemberAmerigroupIDDto dto = result.get(0);
        assertEquals("123456", dto.amerigroupID);
        assertEquals("G001", dto.groupID);
    }
}





****************

    package org.example;

import org.example.dto.FacetsMemberAmerigroupIDDto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test") // Ensures H2 properties are used
class FacetsMemberDaoImplTest {

    @Autowired
    private FacetsMemberDaoImpl facetsMemberDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testGetIdAndPrefixBySbsbIdDateAndPrefix() {
        // Insert test data
        jdbcTemplate.execute("CREATE TABLE cmc_sbsb_subsc (SBSB_ID VARCHAR(10), GRGR_CK VARCHAR(10), SBSB_CK INT PRIMARY KEY)");
        jdbcTemplate.execute("INSERT INTO cmc_sbsb_subsc (SBSB_ID, GRGR_CK, SBSB_CK) VALUES ('123456', 'G001', 1)");

        jdbcTemplate.execute("CREATE TABLE cmc_meme_member (MEME_CK INT PRIMARY KEY, SBSB_CK INT)");
        jdbcTemplate.execute("INSERT INTO cmc_meme_member (MEME_CK, SBSB_CK) VALUES (1, 1)");

        jdbcTemplate.execute("CREATE TABLE cmc_mepe_prcs_elig (MEPE_CK INT PRIMARY KEY, MEME_CK INT, GRGR_CK VARCHAR(10), CSCS_ID VARCHAR(10), CSPI_ID VARCHAR(10), CSPD_CAT VARCHAR(10), MEPE_EFF_DT DATE, MEPE_TERM_DT DATE)");
        jdbcTemplate.execute("INSERT INTO cmc_mepe_prcs_elig (MEPE_CK, MEME_CK, GRGR_CK, CSCS_ID, CSPI_ID, CSPD_CAT, MEPE_EFF_DT, MEPE_TERM_DT) VALUES (1, 1, 'G001', 'C001', 'P001', 'CAT1', '2024-01-01', '2025-12-31')");

        jdbcTemplate.execute("CREATE TABLE CMC_CSPI_CS_PLAN (GRGR_CK VARCHAR(10), CSCS_ID VARCHAR(10), CSPI_ID VARCHAR(10), CSPD_CAT VARCHAR(10), CSPI_ITS_PREFIX VARCHAR(10), CSPI_EFF_DT DATE, CSPI_TERM_DT DATE)");
        jdbcTemplate.execute("INSERT INTO CMC_CSPI_CS_PLAN (GRGR_CK, CSCS_ID, CSPI_ID, CSPD_CAT, CSPI_ITS_PREFIX, CSPI_EFF_DT, CSPI_TERM_DT) VALUES ('G001', 'C001', 'P001', 'CAT1', 'G001', '2024-01-01', '2025-12-31')");

        // Execute test
        List<FacetsMemberAmerigroupIDDto> result = facetsMemberDao.getIdAndPrefixBySbsbIdDateAndPrefix("123456", "G001", new Date(), new Date());

        // Assertions
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals("123456", result.get(0).amerigroupID);
        assertEquals("G001", result.get(0).groupID);
    }
}


application-test.properties


# H2 Database Configuration
spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.datasource.platform=h2
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true
spring.h2.console.path=/h2-console

    




    
