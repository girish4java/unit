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