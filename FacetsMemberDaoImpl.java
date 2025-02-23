
// CHECKSTYLE:OFF
package com.amerigroup.facets.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.*;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import com.amerigroup.utilities.Text;
import com.amerigroup.daobase.ADatabaseDao;
import com.amerigroup.daobase.IDatabaseDao;
import com.amerigroup.daobase.DAOTransaction;
import com.amerigroup.exception.runtime.execution.DAOException;
import com.amerigroup.exception.checked.DAONeedRollbackException;
import com.amerigroup.utilities.EnvironmentResolver;
import org.apache.log4j.Logger;

import com.amerigroup.facets.dao.dto.*;

/**
 * <p>A DAO for facets Members</p>
 * <p>This class is the standard implementation for the DAO.</p>
 * <p>Definition filename: daoFacetsMember.xml</p>
 */
@SuppressWarnings("all")
public class FacetsMemberDaoImpl extends ADatabaseDao implements IFacetsMemberDao
{

    /** Log4j logger */
    private static final Logger log = Logger.getLogger(FacetsMemberDaoImpl.class);

    /**
     * {@inheritDoc}
     * @see com.amerigroup.dao.ADatabaseDao#getLogger()
     */
    @Override
    public Logger getLogger()
    {
    	return FacetsMemberDaoImpl.log;
    }
    
	/**
	 * {@inheritDoc}
	 * @see com.amerigroup.dao.IDatabaseDao#getDatasourceJndiName()
	 */
	 public String getDatasourceJndiName() 
   	 {  
       	 String jndiName = "";
     	 if (!Text.isEffectivelyEmptyOrNull(jndiName)){
	        	return jndiName;
	  	 } 	        
	  	 return getJndiNameByEnvironment();
   	 }
    
    public String getJndiNameByEnvironment() {
        
    	com.amerigroup.utilities.EnvironmentResolver envResolver = new com.amerigroup.utilities.EnvironmentResolver(); 
    	String envID = envResolver.getEnvironmentId();
		log.info("datasource " + envID);
		
		if(envID != null)
    	{

			envID= envResolver.getSingleProperty(envID,com.amerigroup.utilities.EnvironmentResolver.PropertyType.PROPERTY_TYPE_DATASOURCE, "Facets");
        }
        
        return envID;      
    	
    }
	
		
    /**
     * <p>get sbsb ID and prefix as of search Date</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select sbsb.SBSB_ID, sbsb.GRGR_CK
				from cmc_sbsb_subsc sbsb
				JOIN cmc_meme_member meme ON sbsb.sbsb_ck = meme.sbsb_ck
				JOIN cmc_mepe_prcs_elig mepe ON mepe.meme_ck = meme.meme_ck
				JOIN CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = mepe.GRGR_CK and cspi.cscs_ID=mepe.cscs_ID and cspi.cspi_ID=mepe.cspi_ID
				and cspi.CSPD_CAT=mepe.CSPD_CAT and mepe.MEPE_EFF_DT <= cspi.CSPI_TERM_DT and mepe.MEPE_TERM_DT >= cspi.CSPI_EFF_DT
				where sbsb.SBSB_ID=? and upper(cspi.CSPI_ITS_PREFIX)=?
				and ?  <= cspi.CSPI_TERM_DT and ? >= cspi.CSPI_EFF_DT
			</pre></blockquote></p>
     * @param subscriberID The SBSB_ID of the member
     * @param prefix The prefix
     * @param searchStartDate The Search Date
     * @param searchEndDate The Search Date
     * @return A <tt>List</tt> of <tt>FacetsMemberAmerigroupIDDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberAmerigroupIDDto> getIdAndPrefixBySbsbIdDateAndPrefix(String subscriberID, String prefix, Date searchStartDate, Date searchEndDate)
    {
        List<FacetsMemberAmerigroupIDDto> result = new ArrayList<FacetsMemberAmerigroupIDDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select sbsb.SBSB_ID, sbsb.GRGR_CK"+
         " from cmc_sbsb_subsc sbsb"+
         " JOIN cmc_meme_member meme ON sbsb.sbsb_ck = meme.sbsb_ck"+
         " JOIN cmc_mepe_prcs_elig mepe ON mepe.meme_ck = meme.meme_ck"+
         " JOIN CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = mepe.GRGR_CK and cspi.cscs_ID=mepe.cscs_ID and cspi.cspi_ID=mepe.cspi_ID"+
         " and cspi.CSPD_CAT=mepe.CSPD_CAT and mepe.MEPE_EFF_DT <= cspi.CSPI_TERM_DT and mepe.MEPE_TERM_DT >= cspi.CSPI_EFF_DT"+
         " where sbsb.SBSB_ID=? and upper(cspi.CSPI_ITS_PREFIX)=?"+
         " and ?  <= cspi.CSPI_TERM_DT and ? >= cspi.CSPI_EFF_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getIdAndPrefixBySbsbIdDateAndPrefix" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (subscriberID) to " + subscriberID);
                
            ps.setString(1, subscriberID);
            
                log.debug("   Setting parm #2 (prefix) to " + prefix);
                
            ps.setString(2, prefix);
            
                log.debug("   Setting parm #3 (searchStartDate) to " + searchStartDate);
                

				if (searchStartDate == null) 
            {
                
                ps.setTimestamp(3, null);
                
            }
            else {
                
                ps.setTimestamp(3, new java.sql.Timestamp(searchStartDate.getTime()));
                
            }            
            
                log.debug("   Setting parm #4 (searchEndDate) to " + searchEndDate);
                

				if (searchEndDate == null) 
            {
                
                ps.setTimestamp(4, null);
                
            }
            else {
                
                ps.setTimestamp(4, new java.sql.Timestamp(searchEndDate.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberAmerigroupIDDto dto = new FacetsMemberAmerigroupIDDto();
                                
                dto.amerigroupID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.amerigroupID = null;
                }
                                
                dto.groupID = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getIdAndPrefixBySbsbIdDateAndPrefix  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Find amerigroupID using the amerigroup id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre> select sbsb_id, grgr_ck from cmc_sbsb_subsc where sbsb_id=? </pre></blockquote></p>
     * @param agp Amerigroup ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberAmerigroupIDDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberAmerigroupIDDto> findByAgp(String agp)
    {
        List<FacetsMemberAmerigroupIDDto> result = new ArrayList<FacetsMemberAmerigroupIDDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = " select sbsb_id, grgr_ck from cmc_sbsb_subsc where sbsb_id=? ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<findByAgp" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (agp) to " + agp);
                
            ps.setString(parmNum++, agp);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberAmerigroupIDDto dto = new FacetsMemberAmerigroupIDDto();
                                
                dto.amerigroupID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.amerigroupID = null;
                }
                                
                dto.groupID = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:findByAgp  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members past, current, and future eligibility using the amerigroup id, sorted with most to least recent</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where s.SBSB_ID =?
                order by e.MEPE_EFF_DT DESC
            </pre></blockquote></p>
     * @param amerigroupID Amerigroup ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilitiesByAmerigroupID(String amerigroupID)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where s.SBSB_ID =?"+
         " order by e.MEPE_EFF_DT DESC"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilitiesByAmerigroupID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (amerigroupID) to " + amerigroupID);
                
            ps.setString(parmNum++, amerigroupID);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilitiesByAmerigroupID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') and g.grgr_id like 'INMCD%' order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityByINMedicaidID(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') and g.grgr_id like 'INMCD%' order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityByINMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityByINMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityByMedicaidID(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityByMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityByMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD', 'MDCR') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityByMedicaidIDWithMDCRGrpType(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD', 'MDCR') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityByMedicaidIDWithMDCRGrpType" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityByMedicaidIDWithMDCRGrpType  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicare id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_HICN =?)  and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicareId Medicare ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityByCurrentMedicareID(String medicareId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_HICN =?)  and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityByCurrentMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityByCurrentMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicare id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' 
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where x.MECR_NO_ORIG = ? and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicareId Medicare ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityByOldMedicareID(String medicareId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where x.MECR_NO_ORIG = ? and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityByOldMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityByOldMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members past, current, and future eligibility using the amerigroup id, sorted with most to least recent</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where s.SBSB_ID =?
                order by e.MEPE_EFF_DT DESC
            </pre></blockquote></p>
     * @param amerigroupID Amerigroup ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilitiesWithNoEligibilityCheckByAmerigroupID(String amerigroupID)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where s.SBSB_ID =?"+
         " order by e.MEPE_EFF_DT DESC"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilitiesWithNoEligibilityCheckByAmerigroupID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (amerigroupID) to " + amerigroupID);
                
            ps.setString(parmNum++, amerigroupID);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilitiesWithNoEligibilityCheckByAmerigroupID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') and g.grgr_id like 'INMCD%' order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityWithNoEligibilityCheckByINMedicaidID(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') and g.grgr_id like 'INMCD%' order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityWithNoEligibilityCheckByINMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityWithNoEligibilityCheckByINMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityWithNoEligibilityCheckByMedicaidID(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityWithNoEligibilityCheckByMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityWithNoEligibilityCheckByMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD', 'MDCR') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityWithNoEligibilityCheckByMedicaidIDWithMdcrGrpType(String medicaidId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and g.grgr_mctr_type in ('MDCD', 'MDDD', 'MDCR') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityWithNoEligibilityCheckByMedicaidIDWithMdcrGrpType" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityWithNoEligibilityCheckByMedicaidIDWithMdcrGrpType  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicare id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where (m.MEME_HICN =?) and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicareId Medicare ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityWithNoEligibilityCheckByCurrentMedicareID(String medicareId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_HICN =?) and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityWithNoEligibilityCheckByCurrentMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityWithNoEligibilityCheckByCurrentMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicare id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			    select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT, 
			    e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
                from CMC_MEPE_PRCS_ELIG e 
                inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK  
                inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK 
                left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK
                left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK 
                left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK 
                left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd 
                left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
                LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
                left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
                left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
                where x.MECR_NO_ORIG = ? and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc
            </pre></blockquote></p>
     * @param medicareId Medicare ID of the member
     * @return A <tt>List</tt> of <tt>FacetsMemberEligibilityDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberEligibilityDto> getAllEligibilityWithNoEligibilityCheckByOldMedicareID(String medicareId)
    {
        List<FacetsMemberEligibilityDto> result = new ArrayList<FacetsMemberEligibilityDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where x.MECR_NO_ORIG = ? and g.grgr_mctr_type in ('MDCR', 'MDDD') order by e.MEPE_EFF_DT desc"+
         "             ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getAllEligibilityWithNoEligibilityCheckByOldMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getAllEligibilityWithNoEligibilityCheckByOldMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get the home address for a member</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>select a.sbad_type, a.sbad_addr1, a.sbad_addr2, a.sbad_addr3, a.sbad_city, a.sbad_county, a.sbad_state, a.sbad_zip, 
						a.sbad_phone, a.sbad_phone_ext, a.sbad_email, a.sbad_fax, a.sbad_fax_ext, s.SBAD_TYPE_HOME, s.SBAD_TYPE_MAIL   
				  from CMC_SBSB_SUBSC s
				  join CMC_SBAD_ADDR a on a.SBSB_CK = s.SBSB_CK and a.SBAD_TYPE = s.SBAD_TYPE_HOME
				where s.sbsb_id = ?
			</pre></blockquote></p>
     * @param agpID The Amerigroup ID of the member
     * @return the <tt>FacetsMemberAddressDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberAddressDto getHomeAddress(String agpID)
    {
        FacetsMemberAddressDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = "select a.sbad_type, a.sbad_addr1, a.sbad_addr2, a.sbad_addr3, a.sbad_city, a.sbad_county, a.sbad_state, a.sbad_zip,"+
         " a.sbad_phone, a.sbad_phone_ext, a.sbad_email, a.sbad_fax, a.sbad_fax_ext, s.SBAD_TYPE_HOME, s.SBAD_TYPE_MAIL"+
         " from CMC_SBSB_SUBSC s"+
         " join CMC_SBAD_ADDR a on a.SBSB_CK = s.SBSB_CK and a.SBAD_TYPE = s.SBAD_TYPE_HOME"+
         " where s.sbsb_id = ?"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getHomeAddress" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (agpID) to " + agpID);
                
            ps.setString(parmNum++, agpID);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberAddressDto dto = new FacetsMemberAddressDto();
                                
                dto.type = rs.getString("sbad_type");
                if (rs.wasNull()) 
                {
                    dto.type = null;
                }
                                
                dto.street1 = rs.getString("sbad_addr1");
                if (rs.wasNull()) 
                {
                    dto.street1 = null;
                }
                                
                dto.street2 = rs.getString("sbad_addr2");
                if (rs.wasNull()) 
                {
                    dto.street2 = null;
                }
                                
                dto.street3 = rs.getString("sbad_addr3");
                if (rs.wasNull()) 
                {
                    dto.street3 = null;
                }
                                
                dto.city = rs.getString("sbad_city");
                if (rs.wasNull()) 
                {
                    dto.city = null;
                }
                                
                dto.county = rs.getString("sbad_county");
                if (rs.wasNull()) 
                {
                    dto.county = null;
                }
                                
                dto.state = rs.getString("sbad_state");
                if (rs.wasNull()) 
                {
                    dto.state = null;
                }
                                
                dto.zip = rs.getString("sbad_zip");
                if (rs.wasNull()) 
                {
                    dto.zip = null;
                }
                                
                dto.phone = rs.getString("sbad_phone");
                if (rs.wasNull()) 
                {
                    dto.phone = null;
                }
                                
                dto.phoneExt = rs.getString("sbad_phone_ext");
                if (rs.wasNull()) 
                {
                    dto.phoneExt = null;
                }
                                
                dto.email = rs.getString("sbad_email");
                if (rs.wasNull()) 
                {
                    dto.email = null;
                }
                                
                dto.fax = rs.getString("sbad_fax");
                if (rs.wasNull()) 
                {
                    dto.fax = null;
                }
                                
                dto.faxExt = rs.getString("sbad_fax_ext");
                if (rs.wasNull()) 
                {
                    dto.faxExt = null;
                }
                                
                dto.homeAddrType = rs.getString("SBAD_TYPE_HOME");
                if (rs.wasNull()) 
                {
                    dto.homeAddrType = null;
                }
                                
                dto.mailAddrType = rs.getString("SBAD_TYPE_MAIL");
                if (rs.wasNull()) 
                {
                    dto.mailAddrType = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getHomeAddress  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get the Dual Citizenship details by Subscriber ID</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			WITH TEMP AS
			(
				SELECT DISTINCT 
		            DP.dual_link, DP.dual_type, 
	            	SUB.sbsb_id as requestedsbsbId,
	            	GRP.grgr_id as requestedgrgrId, 
	            	MEM.meme_ck as requestedmemeCk,
	            	SUB2.sbsb_id as targetsbsbId, 
	            	GRP2.grgr_id as targetgrgrId,
	            	MEM2.meme_ck as targetmemeCk,
	            	DP.DUAL_SPN_START_DT, DP.DUAL_SPN_END_DT,
	            	DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT 
				FROM FACETS.CMC_MEME_MEMBER MEM 
	 			INNER JOIN FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck
	   			INNER JOIN FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck
	    		INNER JOIN AGP.DUAL_POPULATION_SPN DP on DP.meme_ck = MEM.meme_ck  AND DP.DUAL_LINK > 0
	    		INNER JOIN AGP.DUAL_POPULATION_SPN DP2 on DP2.dual_link = DP.dual_link AND dp2.meme_ck != dp.meme_ck AND DP2.DUAL_LINK > 0
		        INNER JOIN AGP.DUAL_DESC DD on DD.dual_type = DP.dual_type 
		        LEFT OUTER JOIN AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DP.dual_pop_spn_id = DMACS.dual_spn_id 
	    		INNER JOIN FACETS.CMC_MEME_MEMBER MEM2 on MEM2.meme_ck = DP2.meme_ck 
	   			INNER JOIN FACETS.CMC_SBSB_SUBSC SUB2 on SUB2.sbsb_ck = MEM2.sbsb_ck
	    		INNER JOIN FACETS.CMC_GRGR_GROUP GRP2 on GRP2.grgr_ck = MEM2.grgr_ck 
				WHERE SUB.sbsb_id = ?
				
	    		UNION 
	    		
				SELECT DISTINCT 
		            0, DE.dual_type, 
	            	SUB.sbsb_id as requestedsbsbId,
	            	GRP.grgr_id as requestedgrgrId, 
	            	MEM.meme_ck as requestedmemeCk,
	            	'' as targetsbsbId, 
	            	'' as targetgrgrId,
	            	0 as targetmemeCk,
	            	DE.DUAL_SPN_START_DT, DE.DUAL_SPN_END_DT,
					DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT 
				FROM FACETS.CMC_MEME_MEMBER MEM 
	     		INNER JOIN FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck 
	     		INNER JOIN FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck
				INNER JOIN AGP.DUAL_ELIGIBLE_SPN DE on DE.meme_ck = MEM.meme_ck
		        INNER JOIN AGP.DUAL_DESC DD on DD.dual_type = DE.dual_type 
		        LEFT OUTER JOIN AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DE.dual_elig_spn_id = DMACS.dual_spn_id 
				WHERE SUB.sbsb_id = ?
				
				UNION
				
				select DISTINCT DP.dual_link, DP.dual_type,SUB.sbsb_id as requestedsbsbId,GRP.grgr_id as requestedgrgrId,
        MEM.meme_ck as requestedmemeCk,'' as targetsbsbId,'' as targetgrgrId,0 as targetmemeCk,DP.DUAL_SPN_START_DT, DP.DUAL_SPN_END_DT,DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT
        from FACETS.CMC_MEME_MEMBER MEM inner join FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck 
        inner join FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck 
        inner join AGP.DUAL_POPULATION_SPN DP on DP.meme_ck = MEM.meme_ck and dual_link=-1 and dual_type='I'
        inner join AGP.DUAL_DESC DD on DD.dual_type = DP.dual_type
        left outer join AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DP.dual_pop_spn_id = DMACS.dual_spn_id
        where SUB.sbsb_id = ?
				)
        		SELECT t.* FROM TEMP t 
        		order by t.DUAL_SPN_END_DT desc, t.dual_type ASC
			</pre></blockquote></p>
     * @param sbsbId Subscriber Id
     * @return A <tt>List</tt> of <tt>FacetsMemberDualCitizenshipDetailsDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberDualCitizenshipDetailsDto> getDualCitizenshipDetailsBySBSBIDwithMediciad(String sbsbId)
    {
        List<FacetsMemberDualCitizenshipDetailsDto> result = new ArrayList<FacetsMemberDualCitizenshipDetailsDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " WITH TEMP AS"+
         " ("+
         " SELECT DISTINCT"+
         " DP.dual_link, DP.dual_type,"+
         " SUB.sbsb_id as requestedsbsbId,"+
         " GRP.grgr_id as requestedgrgrId,"+
         " MEM.meme_ck as requestedmemeCk,"+
         " SUB2.sbsb_id as targetsbsbId,"+
         " GRP2.grgr_id as targetgrgrId,"+
         " MEM2.meme_ck as targetmemeCk,"+
         " DP.DUAL_SPN_START_DT, DP.DUAL_SPN_END_DT,"+
         " DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT"+
         " FROM FACETS.CMC_MEME_MEMBER MEM"+
         " INNER JOIN FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck"+
         " INNER JOIN FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck"+
         " INNER JOIN AGP.DUAL_POPULATION_SPN DP on DP.meme_ck = MEM.meme_ck  AND DP.DUAL_LINK > 0"+
         " INNER JOIN AGP.DUAL_POPULATION_SPN DP2 on DP2.dual_link = DP.dual_link AND dp2.meme_ck != dp.meme_ck AND DP2.DUAL_LINK > 0"+
         " INNER JOIN AGP.DUAL_DESC DD on DD.dual_type = DP.dual_type"+
         " LEFT OUTER JOIN AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DP.dual_pop_spn_id = DMACS.dual_spn_id"+
         " INNER JOIN FACETS.CMC_MEME_MEMBER MEM2 on MEM2.meme_ck = DP2.meme_ck"+
         " INNER JOIN FACETS.CMC_SBSB_SUBSC SUB2 on SUB2.sbsb_ck = MEM2.sbsb_ck"+
         " INNER JOIN FACETS.CMC_GRGR_GROUP GRP2 on GRP2.grgr_ck = MEM2.grgr_ck"+
         " WHERE SUB.sbsb_id = ?"+
         " "+
         " UNION"+
         " "+
         " SELECT DISTINCT"+
         " 0, DE.dual_type,"+
         " SUB.sbsb_id as requestedsbsbId,"+
         " GRP.grgr_id as requestedgrgrId,"+
         " MEM.meme_ck as requestedmemeCk,"+
         " '' as targetsbsbId,"+
         " '' as targetgrgrId,"+
         " 0 as targetmemeCk,"+
         " DE.DUAL_SPN_START_DT, DE.DUAL_SPN_END_DT,"+
         " DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT"+
         " FROM FACETS.CMC_MEME_MEMBER MEM"+
         " INNER JOIN FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck"+
         " INNER JOIN FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck"+
         " INNER JOIN AGP.DUAL_ELIGIBLE_SPN DE on DE.meme_ck = MEM.meme_ck"+
         " INNER JOIN AGP.DUAL_DESC DD on DD.dual_type = DE.dual_type"+
         " LEFT OUTER JOIN AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DE.dual_elig_spn_id = DMACS.dual_spn_id"+
         " WHERE SUB.sbsb_id = ?"+
         " "+
         " UNION"+
         " "+
         " select DISTINCT DP.dual_link, DP.dual_type,SUB.sbsb_id as requestedsbsbId,GRP.grgr_id as requestedgrgrId,"+
         " MEM.meme_ck as requestedmemeCk,'' as targetsbsbId,'' as targetgrgrId,0 as targetmemeCk,DP.DUAL_SPN_START_DT, DP.DUAL_SPN_END_DT,DD.dual_desc, MEDICAID_LEVEL, COST_SHARE_CAT"+
         " from FACETS.CMC_MEME_MEMBER MEM inner join FACETS.CMC_SBSB_SUBSC SUB on SUB.sbsb_ck = MEM.sbsb_ck"+
         " inner join FACETS.CMC_GRGR_GROUP GRP on GRP.grgr_ck = MEM.grgr_ck"+
         " inner join AGP.DUAL_POPULATION_SPN DP on DP.meme_ck = MEM.meme_ck and dual_link=-1 and dual_type='I'"+
         " inner join AGP.DUAL_DESC DD on DD.dual_type = DP.dual_type"+
         " left outer join AGP.DUAL_MEDICAID_AID_CAT_SPN DMACS on DP.dual_pop_spn_id = DMACS.dual_spn_id"+
         " where SUB.sbsb_id = ?"+
         " )"+
         " SELECT t.* FROM TEMP t"+
         " order by t.DUAL_SPN_END_DT desc, t.dual_type ASC"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getDualCitizenshipDetailsBySBSBIDwithMediciad" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (sbsbId) to " + sbsbId);
                
                log.debug("   Setting parm #2 (sbsbId) to " + sbsbId);
                
                log.debug("   Setting parm #3 (sbsbId) to " + sbsbId);
                
            ps.setString(1, sbsbId);
            
            ps.setString(2, sbsbId);
            
            ps.setString(3, sbsbId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberDualCitizenshipDetailsDto dto = new FacetsMemberDualCitizenshipDetailsDto();
                                
                dto.dualLink = rs.getString("dual_link");
                if (rs.wasNull()) 
                {
                    dto.dualLink = null;
                }
                                
                dto.dualType = rs.getString("dual_type");
                if (rs.wasNull()) 
                {
                    dto.dualType = null;
                }
                                
                dto.requestedSubscriberId = rs.getString("requestedsbsbId");
                if (rs.wasNull()) 
                {
                    dto.requestedSubscriberId = null;
                }
                                
                dto.requestedGroupId = rs.getString("requestedgrgrId");
                if (rs.wasNull()) 
                {
                    dto.requestedGroupId = null;
                }
                                
                dto.requestedMemeCk = rs.getString("requestedmemeCk");
                if (rs.wasNull()) 
                {
                    dto.requestedMemeCk = null;
                }
                                
                dto.targetSubscriberId = rs.getString("targetsbsbId");
                if (rs.wasNull()) 
                {
                    dto.targetSubscriberId = null;
                }
                                
                dto.targetGroupId = rs.getString("targetgrgrId");
                if (rs.wasNull()) 
                {
                    dto.targetGroupId = null;
                }
                                
                dto.targetMemeCk = rs.getString("targetmemeCk");
                if (rs.wasNull()) 
                {
                    dto.targetMemeCk = null;
                }
                
                dto.dualSpanStartDt = rs.getTimestamp("DUAL_SPN_START_DT");
                if (rs.wasNull()) 
                {
                    dto.dualSpanStartDt = null;
                }
                
                dto.dualSpanEndDt = rs.getTimestamp("DUAL_SPN_END_DT");
                if (rs.wasNull()) 
                {
                    dto.dualSpanEndDt = null;
                }
                                
                dto.dualTypeDesc = rs.getString("dual_desc");
                if (rs.wasNull()) 
                {
                    dto.dualTypeDesc = null;
                }
                                
                dto.medicaidAidCategory = rs.getString("MEDICAID_LEVEL");
                if (rs.wasNull()) 
                {
                    dto.medicaidAidCategory = null;
                }
                                
                dto.medicaidCostShare = rs.getString("COST_SHARE_CAT");
                if (rs.wasNull()) 
                {
                    dto.medicaidCostShare = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getDualCitizenshipDetailsBySBSBIDwithMediciad  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>populate the Dual Citizenship AID Category name based on LAST_VER_DATEs and CREATE_DTM by MEME ID</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			select  MEDICAID_LVL   
			from AGP.MCR_ENR_MEDICAID_LEVEL MEML 
			where meme_ck = ? 
			order by LAST_VER_DATE desc, create_dtm desc 
			</pre></blockquote></p>
     * @param memeCk member contrived key
     * @return A <tt>List</tt> of <tt>FacetsMemberDualCitizenshipAIDCategoryDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberDualCitizenshipAIDCategoryDto> getDualCitizenshipAIDCategoryByMemeCk(String memeCk)
    {
        List<FacetsMemberDualCitizenshipAIDCategoryDto> result = new ArrayList<FacetsMemberDualCitizenshipAIDCategoryDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select  MEDICAID_LVL"+
         " from AGP.MCR_ENR_MEDICAID_LEVEL MEML"+
         " where meme_ck = ?"+
         " order by LAST_VER_DATE desc, create_dtm desc"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getDualCitizenshipAIDCategoryByMemeCk" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (memeCk) to " + memeCk);
                
            ps.setString(1, memeCk);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberDualCitizenshipAIDCategoryDto dto = new FacetsMemberDualCitizenshipAIDCategoryDto();
                                
                dto.medicaidAidCategory = rs.getString("MEDICAID_LVL");
                if (rs.wasNull()) 
                {
                    dto.medicaidAidCategory = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getDualCitizenshipAIDCategoryByMemeCk  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Fetches the member details by sbrUid(SBSB_CK from Facets), eligibility effective date and eligibility termination date</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			select
				m.MEME_FIRST_NAME , m.MEME_MID_INIT, m.MEME_LAST_NAME ,
				m.MEME_BIRTH_DT , m.MEME_SEX ,
				m.MEME_CK , m.MEME_HICN , m.MEME_MEDCD_NO, m.SBSB_CK,
				cspi.CSPI_ITS_PREFIX ,e.MEPE_EFF_DT , e.MEPE_TERM_DT ,
				e.PDPD_ID , e.CSPI_ID , e.CSCS_ID, e.MEPE_ELIG_IND ,
				s.SBSB_ID ,pd.LOBD_ID,g.grgr_ck, g.CICI_ID , g.GRGR_ID, 
				g.GRGR_MCTR_TYPE, sg.SGSG_MCTR_TYPE, sg.sgsg_ck
			from CMC_SBSB_SUBSC s
			inner join CMC_MEME_MEMBER m on m.SBSB_CK = s.SBSB_CK
			inner join CMC_MEPE_PRCS_ELIG e on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'
			inner join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK
			inner join FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
			inner join CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = e.GRGR_CK and cspi.CSCS_ID = e.CSCS_ID and cspi.CSPI_ID = e.CSPI_ID and cspi.CSPD_CAT = e.CSPD_CAT and e.MEPE_EFF_DT between cspi.CSPI_EFF_DT and cspi.CSPI_TERM_DT
			left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
			where s.sbsb_ck = ? and e.MEPE_EFF_DT = ? and e.MEPE_TERM_DT = ?
		</pre></blockquote></p>
     * @param sbrUid Member's Subscriber CK(SBSB_CK from Facets)
     * @param eligibilityEffDt Member's Eligibility Effective Date
     * @param eligibilityTermDt Member's Eligibility Termination Date
     * @return the <tt>FacetsMemberMemberDetailsDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberMemberDetailsDto getEligibilityBySbrUidAndEligibilityDates(String sbrUid, Date eligibilityEffDt, Date eligibilityTermDt)
    {
        FacetsMemberMemberDetailsDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select"+
         " m.MEME_FIRST_NAME , m.MEME_MID_INIT, m.MEME_LAST_NAME ,"+
         " m.MEME_BIRTH_DT , m.MEME_SEX ,"+
         " m.MEME_CK , m.MEME_HICN , m.MEME_MEDCD_NO, m.SBSB_CK,"+
         " cspi.CSPI_ITS_PREFIX ,e.MEPE_EFF_DT , e.MEPE_TERM_DT ,"+
         " e.PDPD_ID , e.CSPI_ID , e.CSCS_ID, e.MEPE_ELIG_IND ,"+
         " s.SBSB_ID ,pd.LOBD_ID,g.grgr_ck, g.CICI_ID , g.GRGR_ID,"+
         " g.GRGR_MCTR_TYPE, sg.SGSG_MCTR_TYPE, sg.sgsg_ck"+
         " from CMC_SBSB_SUBSC s"+
         " inner join CMC_MEME_MEMBER m on m.SBSB_CK = s.SBSB_CK"+
         " inner join CMC_MEPE_PRCS_ELIG e on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_GRGR_GROUP g on e.GRGR_CK = g.GRGR_CK"+
         " inner join FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " inner join CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = e.GRGR_CK and cspi.CSCS_ID = e.CSCS_ID and cspi.CSPI_ID = e.CSPI_ID and cspi.CSPD_CAT = e.CSPD_CAT and e.MEPE_EFF_DT between cspi.CSPI_EFF_DT and cspi.CSPI_TERM_DT"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " where s.sbsb_ck = ? and e.MEPE_EFF_DT = ? and e.MEPE_TERM_DT = ?"+
         " 		";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getEligibilityBySbrUidAndEligibilityDates" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (sbrUid) to " + sbrUid);
                
            ps.setString(parmNum++, sbrUid);
            
				log.debug("   Setting parm #" + parmNum + " (eligibilityEffDt) to " + eligibilityEffDt);
                

				if (eligibilityEffDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligibilityEffDt.getTime()));
                
            }            
            
				log.debug("   Setting parm #" + parmNum + " (eligibilityTermDt) to " + eligibilityTermDt);
                

				if (eligibilityTermDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligibilityTermDt.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberMemberDetailsDto dto = new FacetsMemberMemberDetailsDto();
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.middleInitial = rs.getString("MEME_MID_INIT");
                if (rs.wasNull()) 
                {
                    dto.middleInitial = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                
                dto.birthDate = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDate = null;
                }
                                
                dto.gender = rs.getString("MEME_SEX");
                if (rs.wasNull()) 
                {
                    dto.gender = null;
                }
                                
                dto.memberCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memberCk = null;
                }
                                
                dto.medicareId = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareId = null;
                }
                                
                dto.medicaidId = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidId = null;
                }
                                
                dto.sbrUid = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbrUid = null;
                }
                                
                dto.planPrefix = rs.getString("CSPI_ITS_PREFIX");
                if (rs.wasNull()) 
                {
                    dto.planPrefix = null;
                }
                
                dto.eligibilityEffDt = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.eligibilityEffDt = null;
                }
                
                dto.eligibilityTermDt = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.eligibilityTermDt = null;
                }
                                
                dto.productId = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productId = null;
                }
                                
                dto.planId = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planId = null;
                }
                                
                dto.classId = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classId = null;
                }
                                
                dto.eligibilityIndicator = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.eligibilityIndicator = null;
                }
                                
                dto.subscriberId = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.subscriberId = null;
                }
                                
                dto.lineOfBusiness = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lineOfBusiness = null;
                }
                                
                dto.groupCk = rs.getString("grgr_ck");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.region = rs.getString("CICI_ID");
                if (rs.wasNull()) 
                {
                    dto.region = null;
                }
                                
                dto.groupId = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupId = null;
                }
                                
                dto.groupType = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.groupType = null;
                }
                                
                dto.subGroup = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subGroup = null;
                }
                                
                dto.SGSG_CK = rs.getString("SGSG_CK");
                if (rs.wasNull()) 
                {
                    dto.SGSG_CK = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getEligibilityBySbrUidAndEligibilityDates  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get Member Claim Hold Info by sbruid</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
					SELECT S.SBSB_ID, MH.MEME_CK, MH.MECH_EFF_DT, MH.MECH_TERM_DT, MH.CLST_MCTR_REAS, 
						MEPE.MEPE_EFF_DT, MEPE.MEPE_TERM_DT, MEPE.MEPE_ELIG_IND
					FROM CMC_MECH_CLM_HOLD MH
					JOIN CMC_MEME_MEMBER M ON MH.MEME_CK = M.MEME_CK
					JOIN CMC_SBSB_SUBSC S ON S.sbsb_ck = M.sbsb_ck
					JOIN CMC_MEPE_PRCS_ELIG MEPE ON M.MEME_CK = MEPE.MEME_CK 
					WHERE S.SBSB_CK = ?  
						AND MH.CLST_MCTR_REAS = 'RETR' 
						AND MEPE.MEPE_ELIG_IND = 'Y' 
						AND MH.MECH_TERM_DT BETWEEN ? AND ? 
						order by MH.MECH_TERM_DT desc
			</pre></blockquote></p>
     * @param sbruid Member's SBSB_CK
     * @param effDate Member's Elig Eff Date
     * @param termDate Member's Elig Term Date
     * @return A <tt>List</tt> of <tt>FacetsMemberMemberClaimHoldInfoDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberMemberClaimHoldInfoDto> getMemberClaimHoldDetailsBySbruid(String sbruid, Date effDate, Date termDate)
    {
        List<FacetsMemberMemberClaimHoldInfoDto> result = new ArrayList<FacetsMemberMemberClaimHoldInfoDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT S.SBSB_ID, MH.MEME_CK, MH.MECH_EFF_DT, MH.MECH_TERM_DT, MH.CLST_MCTR_REAS,"+
         " MEPE.MEPE_EFF_DT, MEPE.MEPE_TERM_DT, MEPE.MEPE_ELIG_IND"+
         " FROM CMC_MECH_CLM_HOLD MH"+
         " JOIN CMC_MEME_MEMBER M ON MH.MEME_CK = M.MEME_CK"+
         " JOIN CMC_SBSB_SUBSC S ON S.sbsb_ck = M.sbsb_ck"+
         " JOIN CMC_MEPE_PRCS_ELIG MEPE ON M.MEME_CK = MEPE.MEME_CK"+
         " WHERE S.SBSB_CK = ?"+
         " AND MH.CLST_MCTR_REAS = 'RETR'"+
         " AND MEPE.MEPE_ELIG_IND = 'Y'"+
         " AND MH.MECH_TERM_DT BETWEEN ? AND ?"+
         " order by MH.MECH_TERM_DT desc"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getMemberClaimHoldDetailsBySbruid" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (sbruid) to " + sbruid);
                
            ps.setString(1, sbruid);
            
                log.debug("   Setting parm #2 (effDate) to " + effDate);
                

				if (effDate == null) 
            {
                
                ps.setTimestamp(2, null);
                
            }
            else {
                
                ps.setTimestamp(2, new java.sql.Timestamp(effDate.getTime()));
                
            }            
            
                log.debug("   Setting parm #3 (termDate) to " + termDate);
                

				if (termDate == null) 
            {
                
                ps.setTimestamp(3, null);
                
            }
            else {
                
                ps.setTimestamp(3, new java.sql.Timestamp(termDate.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberMemberClaimHoldInfoDto dto = new FacetsMemberMemberClaimHoldInfoDto();
                                
                dto.sbsbId = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbId = null;
                }
                                
                dto.memeCK = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCK = null;
                }
                
                dto.effDate = rs.getTimestamp("MECH_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.effDate = null;
                }
                
                dto.termDate = rs.getTimestamp("MECH_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.termDate = null;
                }
                                
                dto.reasonCode = rs.getString("CLST_MCTR_REAS");
                if (rs.wasNull()) 
                {
                    dto.reasonCode = null;
                }
                
                dto.eligEffDate = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.eligEffDate = null;
                }
                
                dto.eligTermDate = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.eligTermDate = null;
                }
                                
                dto.eligInd = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.eligInd = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getMemberClaimHoldDetailsBySbruid  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>gets the prefix from the given class, plan, grgrCk, cspdCat and with respective elig dates</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select CSPI_ITS_PREFIX from CMC_CSPI_CS_PLAN 
				where ? >= CSPI_EFF_DT and  ? <= CSPI_TERM_DT and GRGR_CK = ? and CSCS_ID = ? and CSPD_CAT = ? and CSPI_ID = ?
				order by CSPI_TERM_DT desc
			</pre></blockquote></p>
     * @param eligTermDt Eligibility Effective Date
     * @param eligEffDt Eligibility Termination Date
     * @param groupCk GRGR_CK
     * @param classId CSCS_ID
     * @param cspdCat CSPD_CAT
     * @param planId CSPI_ID
     * @return the <tt>FacetsMemberClassPlanPrefixDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberClassPlanPrefixDto getPrefixByClassPlan(Date eligTermDt, Date eligEffDt, String groupCk, String classId, String cspdCat, String planId)
    {
        FacetsMemberClassPlanPrefixDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select CSPI_ITS_PREFIX from CMC_CSPI_CS_PLAN"+
         " where ? >= CSPI_EFF_DT and  ? <= CSPI_TERM_DT and GRGR_CK = ? and CSCS_ID = ? and CSPD_CAT = ? and CSPI_ID = ?"+
         " order by CSPI_TERM_DT desc"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getPrefixByClassPlan" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (eligTermDt) to " + eligTermDt);
                

				if (eligTermDt == null) 
            {
                
                ps.setTimestamp(1, null);
                
            }
            else {
                
                ps.setTimestamp(1, new java.sql.Timestamp(eligTermDt.getTime()));
                
            }            
            
                log.debug("   Setting parm #2 (eligEffDt) to " + eligEffDt);
                

				if (eligEffDt == null) 
            {
                
                ps.setTimestamp(2, null);
                
            }
            else {
                
                ps.setTimestamp(2, new java.sql.Timestamp(eligEffDt.getTime()));
                
            }            
            
                log.debug("   Setting parm #3 (groupCk) to " + groupCk);
                
            ps.setString(3, groupCk);
            
                log.debug("   Setting parm #4 (classId) to " + classId);
                
            ps.setString(4, classId);
            
                log.debug("   Setting parm #5 (cspdCat) to " + cspdCat);
                
            ps.setString(5, cspdCat);
            
                log.debug("   Setting parm #6 (planId) to " + planId);
                
            ps.setString(6, planId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberClassPlanPrefixDto dto = new FacetsMemberClassPlanPrefixDto();
                                
                dto.prefix = rs.getString("CSPI_ITS_PREFIX");
                if (rs.wasNull()) 
                {
                    dto.prefix = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getPrefixByClassPlan  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get member medicaid brand by plan</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>SELECT BRAND_CD,BRAND_DESC,GCPP_PATTERN,MARKET_EFF_DT FROM AGP.MEM_MARKET_BRAND where regexp_like (?, gcpp_pattern) and sysdate between MARKET_EFF_DT and MARKET_TERM_DT and MARKET_BRAND_IND = 'Y'
			</pre></blockquote></p>
     * @param GCPProduct a concatenation of group, class, plan and product
     * @return A <tt>List</tt> of <tt>FacetsMemberMarketBrandDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberMarketBrandDto> getMemberMarketBrand(String GCPProduct)
    {
        List<FacetsMemberMarketBrandDto> result = new ArrayList<FacetsMemberMarketBrandDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = "SELECT BRAND_CD,BRAND_DESC,GCPP_PATTERN,MARKET_EFF_DT FROM AGP.MEM_MARKET_BRAND where regexp_like (?, gcpp_pattern) and sysdate between MARKET_EFF_DT and MARKET_TERM_DT and MARKET_BRAND_IND = 'Y'"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getMemberMarketBrand" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (GCPProduct) to " + GCPProduct);
                
            ps.setString(parmNum++, GCPProduct);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberMarketBrandDto dto = new FacetsMemberMarketBrandDto();
                                
                dto.brandCode = rs.getString("BRAND_CD");
                if (rs.wasNull()) 
                {
                    dto.brandCode = null;
                }
                                
                dto.brandDesc = rs.getString("BRAND_DESC");
                if (rs.wasNull()) 
                {
                    dto.brandDesc = null;
                }
                                
                dto.gcppPattern = rs.getString("GCPP_PATTERN");
                if (rs.wasNull()) 
                {
                    dto.gcppPattern = null;
                }
                
                dto.effDate = rs.getTimestamp("MARKET_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.effDate = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getMemberMarketBrand  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get member print brand by plan</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				SELECT BRAND_CD,BRAND_DESC,GCPP_PATTERN,MARKET_EFF_DT 
				FROM AGP.MEM_MARKET_BRAND where regexp_like (?, gcpp_pattern) and sysdate between MARKET_EFF_DT and MARKET_TERM_DT and PRINT_BRAND_IND = 'Y'
			</pre></blockquote></p>
     * @param GCPProduct a concatenation of group, class, plan and product
     * @return A <tt>List</tt> of <tt>FacetsMemberMarketBrandDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberMarketBrandDto> getMemberPrinttBrand(String GCPProduct)
    {
        List<FacetsMemberMarketBrandDto> result = new ArrayList<FacetsMemberMarketBrandDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT BRAND_CD,BRAND_DESC,GCPP_PATTERN,MARKET_EFF_DT"+
         " FROM AGP.MEM_MARKET_BRAND where regexp_like (?, gcpp_pattern) and sysdate between MARKET_EFF_DT and MARKET_TERM_DT and PRINT_BRAND_IND = 'Y'"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getMemberPrinttBrand" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (GCPProduct) to " + GCPProduct);
                
            ps.setString(parmNum++, GCPProduct);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberMarketBrandDto dto = new FacetsMemberMarketBrandDto();
                                
                dto.brandCode = rs.getString("BRAND_CD");
                if (rs.wasNull()) 
                {
                    dto.brandCode = null;
                }
                                
                dto.brandDesc = rs.getString("BRAND_DESC");
                if (rs.wasNull()) 
                {
                    dto.brandDesc = null;
                }
                                
                dto.gcppPattern = rs.getString("GCPP_PATTERN");
                if (rs.wasNull()) 
                {
                    dto.gcppPattern = null;
                }
                
                dto.effDate = rs.getTimestamp("MARKET_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.effDate = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getMemberPrinttBrand  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>get PBP ID from medicare details table with sbsb_ck and eligibility dates</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select distinct memd.MEME_CK ,
				memd.MEMD_EVENT_CD , 
				memd.MEMD_HCFA_EFF_DT, 
				memd.MEMD_HCFA_TERM_DT,
				memd.MEMD_MCTR_PBP
				from CMC_MEMD_MECR_DETL memd 
				join cmc_meme_member meme on meme.meme_ck = memd.meme_ck
				where memd.MEMD_EVENT_CD = 'PBP' and meme.sbsb_ck = ? 
        		and memd.MEMD_HCFA_TERM_DT > ?  and memd.MEMD_HCFA_EFF_DT < ?
				order by MEMD_HCFA_EFF_DT DESC
			</pre></blockquote></p>
     * @param sbsbCk Subscriber's contrived Key
     * @param eligEffDt Eligibility Effective Date
     * @param eligTermDt Eligibility Termination Date
     * @return the <tt>FacetsMemberPBPDetailsDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberPBPDetailsDto getPBPIdBySbrUidAndEligibility(String sbsbCk, Date eligEffDt, Date eligTermDt)
    {
        FacetsMemberPBPDetailsDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select distinct memd.MEME_CK ,"+
         " memd.MEMD_EVENT_CD ,"+
         " memd.MEMD_HCFA_EFF_DT,"+
         " memd.MEMD_HCFA_TERM_DT,"+
         " memd.MEMD_MCTR_PBP"+
         " from CMC_MEMD_MECR_DETL memd"+
         " join cmc_meme_member meme on meme.meme_ck = memd.meme_ck"+
         " where memd.MEMD_EVENT_CD = 'PBP' and meme.sbsb_ck = ?"+
         " and memd.MEMD_HCFA_TERM_DT > ?  and memd.MEMD_HCFA_EFF_DT < ?"+
         " order by MEMD_HCFA_EFF_DT DESC"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getPBPIdBySbrUidAndEligibility" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (sbsbCk) to " + sbsbCk);
                
            ps.setString(parmNum++, sbsbCk);
            
				log.debug("   Setting parm #" + parmNum + " (eligEffDt) to " + eligEffDt);
                

				if (eligEffDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligEffDt.getTime()));
                
            }            
            
				log.debug("   Setting parm #" + parmNum + " (eligTermDt) to " + eligTermDt);
                

				if (eligTermDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligTermDt.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberPBPDetailsDto dto = new FacetsMemberPBPDetailsDto();
                                
                dto.memeCk = rs.getInt("MEME_CK");
                                
                dto.event = rs.getString("MEMD_EVENT_CD");
                if (rs.wasNull()) 
                {
                    dto.event = null;
                }
                
                dto.hcfaEffDT = rs.getTimestamp("MEMD_HCFA_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.hcfaEffDT = null;
                }
                
                dto.hcfaTermDT = rs.getTimestamp("MEMD_HCFA_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.hcfaTermDT = null;
                }
                                
                dto.pbpId = rs.getString("MEMD_MCTR_PBP");
                if (rs.wasNull()) 
                {
                    dto.pbpId = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getPBPIdBySbrUidAndEligibility  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get Member Billing Component ID </p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			select distinct PDBL.PDBL_ID, pdesc.PDBL_DESC
			from CMC_PDBC_PROD_COMP pdbc
			JOIN CMC_PDBL_PROD_BILL PDBL ON pdbc.pdbc_pfx =  pdbl.pdbc_pfx
			left join AGP.PROD_PDBL_DESC pdesc on PDBL.PDBL_ID = pdesc.PDBL_ID
			where pdbc.pdbc_type = 'PDBL'
			and pdbc.pdpd_id = ?
		</pre></blockquote></p>
     * @param pdpdId Members product Id
     * @return A <tt>List</tt> of <tt>FacetsMemberBillingComponentDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberBillingComponentDto> getBillingComponentID(String pdpdId)
    {
        List<FacetsMemberBillingComponentDto> result = new ArrayList<FacetsMemberBillingComponentDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select distinct PDBL.PDBL_ID, pdesc.PDBL_DESC"+
         " from CMC_PDBC_PROD_COMP pdbc"+
         " JOIN CMC_PDBL_PROD_BILL PDBL ON pdbc.pdbc_pfx =  pdbl.pdbc_pfx"+
         " left join AGP.PROD_PDBL_DESC pdesc on PDBL.PDBL_ID = pdesc.PDBL_ID"+
         " where pdbc.pdbc_type = 'PDBL'"+
         " and pdbc.pdpd_id = ?"+
         " 		";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getBillingComponentID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (pdpdId) to " + pdpdId);
                
            ps.setString(parmNum++, pdpdId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberBillingComponentDto dto = new FacetsMemberBillingComponentDto();
                                
                dto.billingComponentId = rs.getString("PDBL_ID");
                if (rs.wasNull()) 
                {
                    dto.billingComponentId = null;
                }
                                
                dto.billingComponentDesc = rs.getString("PDBL_DESC");
                if (rs.wasNull()) 
                {
                    dto.billingComponentDesc = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getBillingComponentID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>get CMS contract ID by sbruid and member eligibility period</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select distinct bgbg.BGBG_ID,
			                    memd.MEMD_HCFA_EFF_DT,
			                    memd.MEMD_HCFA_TERM_DT
				from CMC_MEMD_MECR_DETL memd 
				LEFT JOIN cmc_bgbg_bil_group bgbg on memd.bgbg_ck = bgbg.bgbg_ck 
				join CMC_MEME_MEMBER m on memd.meme_ck = m.meme_ck 
				where memd.MEMD_EVENT_CD = 'SCCC' and m.sbsb_ck = ?  
        		and MEMD_HCFA_TERM_DT > ?  and MEMD_HCFA_EFF_DT < ?
				order by MEMD_HCFA_EFF_DT DESC
			</pre></blockquote></p>
     * @param sbsbCk Subscribers's contrived Key
     * @param eligEffDt Eligibility Effective Date
     * @param eligTermDt Eligibility Termination Date
     * @return the <tt>FacetsMemberBillingGroupDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberBillingGroupDto getCMSContractIdBySbrUidAndEligibilityDates (String sbsbCk, Date eligEffDt, Date eligTermDt)
    {
        FacetsMemberBillingGroupDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select distinct bgbg.BGBG_ID,"+
         " memd.MEMD_HCFA_EFF_DT,"+
         " memd.MEMD_HCFA_TERM_DT"+
         " from CMC_MEMD_MECR_DETL memd"+
         " LEFT JOIN cmc_bgbg_bil_group bgbg on memd.bgbg_ck = bgbg.bgbg_ck"+
         " join CMC_MEME_MEMBER m on memd.meme_ck = m.meme_ck"+
         " where memd.MEMD_EVENT_CD = 'SCCC' and m.sbsb_ck = ?"+
         " and MEMD_HCFA_TERM_DT > ?  and MEMD_HCFA_EFF_DT < ?"+
         " order by MEMD_HCFA_EFF_DT DESC"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCMSContractIdBySbrUidAndEligibilityDates " );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (sbsbCk) to " + sbsbCk);
                
            ps.setString(parmNum++, sbsbCk);
            
				log.debug("   Setting parm #" + parmNum + " (eligEffDt) to " + eligEffDt);
                

				if (eligEffDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligEffDt.getTime()));
                
            }            
            
				log.debug("   Setting parm #" + parmNum + " (eligTermDt) to " + eligTermDt);
                

				if (eligTermDt == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(eligTermDt.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberBillingGroupDto dto = new FacetsMemberBillingGroupDto();
                                
                dto.federalContractId = rs.getString("BGBG_ID");
                if (rs.wasNull()) 
                {
                    dto.federalContractId = null;
                }
                
                dto.hcfaEffDT = rs.getTimestamp("MEMD_HCFA_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.hcfaEffDT = null;
                }
                
                dto.hcfaTermDT = rs.getTimestamp("MEMD_HCFA_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.hcfaTermDT = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCMSContractIdBySbrUidAndEligibilityDates   getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get Member Alert Messages</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
					SELECT DISTINCT			
					       WMDS.WMDS_REC_TYPE,			
					       MEWM.MEWM_EFF_DT,			
					       MEWM.MEWM_TERM_DT,			
					       WMDS.WMDS_SEQ_NO,			
					       WMDS.WMDS_TEXT1,			
					       WMDS.WMDS_TEXT2			
					FROM CMC_SBSB_SUBSC SBSB			
					       JOIN CMC_MEME_MEMBER MEME ON SBSB.SBSB_CK = MEME.SBSB_CK			
					       JOIN CMC_MEWM_ME_MSG MEWM ON MEWM.MEME_CK = MEME.MEME_CK			
					       JOIN CMC_WMDS_DESC WMDS ON WMDS.WMDS_SEQ_NO = MEWM.WMDS_SEQ_NO			
					 WHERE WMDS.WMDS_REC_TYPE = 'MEME' 			
					       AND (MEWM.MEWM_TERM_DT BETWEEN ? AND ? 		
					         OR MEWM.MEWM_EFF_DT BETWEEN ? AND ?  			
					         OR ? BETWEEN MEWM.MEWM_EFF_DT AND MEWM.MEWM_TERM_DT 			
					         OR ? BETWEEN MEWM.MEWM_EFF_DT AND MEWM.MEWM_TERM_DT)			
		                         AND SBSB.SBSB_ID = ?
				</pre></blockquote></p>
     * @param subscriberId The SBSB_ID of the subscriber
     * @param effDate The term date to check on the eligibility
     * @param termDate The term date to check on the provider relationship
     * @return A <tt>List</tt> of <tt>FacetsMemberMemberFullAlertTextDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberMemberFullAlertTextDto> getMemberWarningMsg(String subscriberId, Date effDate, Date termDate)
    {
        List<FacetsMemberMemberFullAlertTextDto> result = new ArrayList<FacetsMemberMemberFullAlertTextDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT DISTINCT"+
         " WMDS.WMDS_REC_TYPE,"+
         " MEWM.MEWM_EFF_DT,"+
         " MEWM.MEWM_TERM_DT,"+
         " WMDS.WMDS_SEQ_NO,"+
         " WMDS.WMDS_TEXT1,"+
         " WMDS.WMDS_TEXT2"+
         " FROM CMC_SBSB_SUBSC SBSB"+
         " JOIN CMC_MEME_MEMBER MEME ON SBSB.SBSB_CK = MEME.SBSB_CK"+
         " JOIN CMC_MEWM_ME_MSG MEWM ON MEWM.MEME_CK = MEME.MEME_CK"+
         " JOIN CMC_WMDS_DESC WMDS ON WMDS.WMDS_SEQ_NO = MEWM.WMDS_SEQ_NO"+
         " WHERE WMDS.WMDS_REC_TYPE = 'MEME'"+
         " AND (MEWM.MEWM_TERM_DT BETWEEN ? AND ?"+
         " OR MEWM.MEWM_EFF_DT BETWEEN ? AND ?"+
         " OR ? BETWEEN MEWM.MEWM_EFF_DT AND MEWM.MEWM_TERM_DT"+
         " OR ? BETWEEN MEWM.MEWM_EFF_DT AND MEWM.MEWM_TERM_DT)"+
         " AND SBSB.SBSB_ID = ?"+
         " 				";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getMemberWarningMsg" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #7 (subscriberId) to " + subscriberId);
                
            ps.setString(7, subscriberId);
            
                log.debug("   Setting parm #1 (effDate) to " + effDate);
                
                log.debug("   Setting parm #3 (effDate) to " + effDate);
                
                log.debug("   Setting parm #5 (effDate) to " + effDate);
                

				if (effDate == null) 
            {
                
                ps.setTimestamp(1, null);
                
                ps.setTimestamp(3, null);
                
                ps.setTimestamp(5, null);
                
            }
            else {
                
                ps.setTimestamp(1, new java.sql.Timestamp(effDate.getTime()));
                
                ps.setTimestamp(3, new java.sql.Timestamp(effDate.getTime()));
                
                ps.setTimestamp(5, new java.sql.Timestamp(effDate.getTime()));
                
            }            
            
                log.debug("   Setting parm #2 (termDate) to " + termDate);
                
                log.debug("   Setting parm #4 (termDate) to " + termDate);
                
                log.debug("   Setting parm #6 (termDate) to " + termDate);
                

				if (termDate == null) 
            {
                
                ps.setTimestamp(2, null);
                
                ps.setTimestamp(4, null);
                
                ps.setTimestamp(6, null);
                
            }
            else {
                
                ps.setTimestamp(2, new java.sql.Timestamp(termDate.getTime()));
                
                ps.setTimestamp(4, new java.sql.Timestamp(termDate.getTime()));
                
                ps.setTimestamp(6, new java.sql.Timestamp(termDate.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberMemberFullAlertTextDto dto = new FacetsMemberMemberFullAlertTextDto();
                                
                dto.recType = rs.getString("wmds_rec_type");
                if (rs.wasNull()) 
                {
                    dto.recType = null;
                }
                
                dto.effDate = rs.getTimestamp("mewm_eff_dt");
                if (rs.wasNull()) 
                {
                    dto.effDate = null;
                }
                
                dto.termDate = rs.getTimestamp("mewm_term_dt");
                if (rs.wasNull()) 
                {
                    dto.termDate = null;
                }
                                
                dto.seqNo = rs.getString("wmds_seq_no");
                if (rs.wasNull()) 
                {
                    dto.seqNo = null;
                }
                                
                dto.text1 = rs.getString("wmds_text1");
                if (rs.wasNull()) 
                {
                    dto.text1 = null;
                }
                                
                dto.text2 = rs.getString("wmds_text2");
                if (rs.wasNull()) 
                {
                    dto.text2 = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getMemberWarningMsg  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get Member Alert Messages</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			SELECT DISTINCT 		
			       WMDS.WMDS_REC_TYPE,		
			       CSPI.CSPI_EFF_DT,		
			       CSPI.CSPI_TERM_DT,		
			       WMDS.WMDS_SEQ_NO,		
			       WMDS.WMDS_TEXT1,		
			       WMDS.WMDS_TEXT2      		
			  FROM CMC_CSPI_CS_PLAN CSPI 		
			       JOIN CMC_WMDS_DESC WMDS ON WMDS.WMDS_SEQ_NO = CSPI.WMDS_SEQ_NO		
			 WHERE WMDS.WMDS_REC_TYPE = 'CSPI'		
			       AND CSPI.GRGR_CK = ?          		
			       AND CSPI.CSCS_ID = ?        		
			       AND CSPI.CSPI_ID = ?   		
                   AND CSPI.PDPD_ID = ?
                   AND (CSPI.CSPI_TERM_DT BETWEEN ? AND ?     
				         OR CSPI.CSPI_EFF_DT BETWEEN ? AND ?      
				         OR ? BETWEEN CSPI.CSPI_EFF_DT AND CSPI.CSPI_TERM_DT    
				         OR ?  BETWEEN CSPI.CSPI_EFF_DT AND CSPI.CSPI_TERM_DT)
                              
		</pre></blockquote></p>
     * @param grgrck The grgrck of the subscriber
     * @param cscsid cscsid of the Plan
     * @param cspiid cspiid on the plan
     * @param pdpdid pdpdid on the plan
     * @param effDate The term date to check on the eligibility
     * @param termDate The term date to check on the provider relationship
     * @return A <tt>List</tt> of <tt>FacetsMemberPlanFullAlertTextDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberPlanFullAlertTextDto> getPlanWarningMsg(String grgrck, String cscsid, String cspiid, String pdpdid, Date effDate, Date termDate)
    {
        List<FacetsMemberPlanFullAlertTextDto> result = new ArrayList<FacetsMemberPlanFullAlertTextDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT DISTINCT"+
         " WMDS.WMDS_REC_TYPE,"+
         " CSPI.CSPI_EFF_DT,"+
         " CSPI.CSPI_TERM_DT,"+
         " WMDS.WMDS_SEQ_NO,"+
         " WMDS.WMDS_TEXT1,"+
         " WMDS.WMDS_TEXT2"+
         " FROM CMC_CSPI_CS_PLAN CSPI"+
         " JOIN CMC_WMDS_DESC WMDS ON WMDS.WMDS_SEQ_NO = CSPI.WMDS_SEQ_NO"+
         " WHERE WMDS.WMDS_REC_TYPE = 'CSPI'"+
         " AND CSPI.GRGR_CK = ?"+
         " AND CSPI.CSCS_ID = ?"+
         " AND CSPI.CSPI_ID = ?"+
         " AND CSPI.PDPD_ID = ?"+
         " AND (CSPI.CSPI_TERM_DT BETWEEN ? AND ?"+
         " OR CSPI.CSPI_EFF_DT BETWEEN ? AND ?"+
         " OR ? BETWEEN CSPI.CSPI_EFF_DT AND CSPI.CSPI_TERM_DT"+
         " OR ?  BETWEEN CSPI.CSPI_EFF_DT AND CSPI.CSPI_TERM_DT)"+
         " "+
         " 		";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getPlanWarningMsg" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (grgrck) to " + grgrck);
                
            ps.setString(1, grgrck);
            
                log.debug("   Setting parm #2 (cscsid) to " + cscsid);
                
            ps.setString(2, cscsid);
            
                log.debug("   Setting parm #3 (cspiid) to " + cspiid);
                
            ps.setString(3, cspiid);
            
                log.debug("   Setting parm #4 (pdpdid) to " + pdpdid);
                
            ps.setString(4, pdpdid);
            
                log.debug("   Setting parm #5 (effDate) to " + effDate);
                
                log.debug("   Setting parm #7 (effDate) to " + effDate);
                
                log.debug("   Setting parm #9 (effDate) to " + effDate);
                

				if (effDate == null) 
            {
                
                ps.setTimestamp(5, null);
                
                ps.setTimestamp(7, null);
                
                ps.setTimestamp(9, null);
                
            }
            else {
                
                ps.setTimestamp(5, new java.sql.Timestamp(effDate.getTime()));
                
                ps.setTimestamp(7, new java.sql.Timestamp(effDate.getTime()));
                
                ps.setTimestamp(9, new java.sql.Timestamp(effDate.getTime()));
                
            }            
            
                log.debug("   Setting parm #6 (termDate) to " + termDate);
                
                log.debug("   Setting parm #8 (termDate) to " + termDate);
                
                log.debug("   Setting parm #10 (termDate) to " + termDate);
                

				if (termDate == null) 
            {
                
                ps.setTimestamp(6, null);
                
                ps.setTimestamp(8, null);
                
                ps.setTimestamp(10, null);
                
            }
            else {
                
                ps.setTimestamp(6, new java.sql.Timestamp(termDate.getTime()));
                
                ps.setTimestamp(8, new java.sql.Timestamp(termDate.getTime()));
                
                ps.setTimestamp(10, new java.sql.Timestamp(termDate.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberPlanFullAlertTextDto dto = new FacetsMemberPlanFullAlertTextDto();
                                
                dto.recType = rs.getString("wmds_rec_type");
                if (rs.wasNull()) 
                {
                    dto.recType = null;
                }
                
                dto.effDate = rs.getTimestamp("cspi_eff_dt");
                if (rs.wasNull()) 
                {
                    dto.effDate = null;
                }
                
                dto.termDate = rs.getTimestamp("cspi_term_dt");
                if (rs.wasNull()) 
                {
                    dto.termDate = null;
                }
                                
                dto.seqNo = rs.getString("wmds_seq_no");
                if (rs.wasNull()) 
                {
                    dto.seqNo = null;
                }
                                
                dto.text1 = rs.getString("wmds_text1");
                if (rs.wasNull()) 
                {
                    dto.text1 = null;
                }
                                
                dto.text2 = rs.getString("wmds_text2");
                if (rs.wasNull()) 
                {
                    dto.text2 = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getPlanWarningMsg  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>fetch members tobacco Status</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
	   SELECT S.CATEGORY_VALUE 
           FROM AGP.ENRL_STATE_REPORTING_CAT S 
           WHERE S.SBSB_CK = ?
             AND S.CATEGORY = 'Tobacco' 
             AND S.CATEGORY_VALUE = ANY ('Y','N','R','U') 
             AND S.CREATED_DT=( 
           SELECT MAX(C.CREATED_DT)  
           FROM AGP.ENRL_STATE_REPORTING_CAT C  
           WHERE C.SBSB_CK = S.SBSB_CK
             AND C.CATEGORY = 'Tobacco' 
             AND C.CATEGORY_VALUE = ANY ('Y','N','R','U') 
              )        
	   		</pre></blockquote></p>
     * @param sbsb_ck Member's SBSB_CK
     * @return the <tt>FacetsMemberTobaccoStatusDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberTobaccoStatusDto getMemberTobaccoStatus(String sbsb_ck)
    {
        FacetsMemberTobaccoStatusDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT S.CATEGORY_VALUE"+
         " FROM AGP.ENRL_STATE_REPORTING_CAT S"+
         " WHERE S.SBSB_CK = ?"+
         " AND S.CATEGORY = 'Tobacco'"+
         " AND S.CATEGORY_VALUE = ANY ('Y','N','R','U')"+
         " AND S.CREATED_DT=("+
         " SELECT MAX(C.CREATED_DT)"+
         " FROM AGP.ENRL_STATE_REPORTING_CAT C"+
         " WHERE C.SBSB_CK = S.SBSB_CK"+
         " AND C.CATEGORY = 'Tobacco'"+
         " AND C.CATEGORY_VALUE = ANY ('Y','N','R','U')"+
         " )"+
         " 	   		";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getMemberTobaccoStatus" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
                log.debug("   Setting parm #1 (sbsb_ck) to " + sbsb_ck);
                
            ps.setString(1, sbsb_ck);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberTobaccoStatusDto dto = new FacetsMemberTobaccoStatusDto();
                                
                dto.categoryValue = rs.getString("CATEGORY_VALUE");
                if (rs.wasNull()) 
                {
                    dto.categoryValue = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getMemberTobaccoStatus  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>get blues prefix</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
			SELECT distinct CSPI_ITS_PREFIX as PREFIX
			FROM CMC_CSPI_CS_PLAN WHERE length(CSPI_ITS_PREFIX)=3			
		   </pre></blockquote></p>
     * @return A <tt>List</tt> of <tt>FacetsMemberBluesPrefixDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberBluesPrefixDto> getBluesPrefixes()
    {
        List<FacetsMemberBluesPrefixDto> result = new ArrayList<FacetsMemberBluesPrefixDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " SELECT distinct CSPI_ITS_PREFIX as PREFIX"+
         " FROM CMC_CSPI_CS_PLAN WHERE length(CSPI_ITS_PREFIX)=3"+
         " 		   ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getBluesPrefixes" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
				log.debug("Executing SQL: " + sql);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberBluesPrefixDto dto = new FacetsMemberBluesPrefixDto();
                                
                dto.prefix = rs.getString("PREFIX");
                if (rs.wasNull()) 
                {
                    dto.prefix = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getBluesPrefixes  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>get sbsb ID and bluse prefix</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select sbsb.SBSB_ID, cspi.CSPI_ITS_PREFIX
				from cmc_sbsb_subsc sbsb
				JOIN cmc_meme_member meme ON sbsb.sbsb_ck = meme.sbsb_ck
				JOIN cmc_mepe_prcs_elig mepe ON mepe.meme_ck = meme.meme_ck and mepe.MEPE_ELIG_IND='Y' 
				JOIN CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = mepe.GRGR_CK and cspi.cscs_ID=mepe.cscs_ID and cspi.cspi_ID=mepe.cspi_ID 
					 and cspi.CSPD_CAT=mepe.CSPD_CAT and length(cspi.CSPI_ITS_PREFIX)=3 and mepe.MEPE_EFF_DT <= cspi.CSPI_TERM_DT and mepe.MEPE_TERM_DT >= cspi.CSPI_EFF_DT
				where sbsb.sbsb_id=? and ? between cspi.CSPI_EFF_DT and cspi.CSPI_TERM_DT			
		    </pre></blockquote></p>
     * @param subscriberID The SBSB_ID of the member
     * @param searchDate The Search Date
     * @return the <tt>FacetsMemberIdAndBluesPrefixDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberIdAndBluesPrefixDto getIdAndBluesPrefixBySbsbIdAndDate(String subscriberID, Date searchDate)
    {
        FacetsMemberIdAndBluesPrefixDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select sbsb.SBSB_ID, cspi.CSPI_ITS_PREFIX"+
         " from cmc_sbsb_subsc sbsb"+
         " JOIN cmc_meme_member meme ON sbsb.sbsb_ck = meme.sbsb_ck"+
         " JOIN cmc_mepe_prcs_elig mepe ON mepe.meme_ck = meme.meme_ck and mepe.MEPE_ELIG_IND='Y'"+
         " JOIN CMC_CSPI_CS_PLAN cspi on cspi.GRGR_CK = mepe.GRGR_CK and cspi.cscs_ID=mepe.cscs_ID and cspi.cspi_ID=mepe.cspi_ID"+
         " and cspi.CSPD_CAT=mepe.CSPD_CAT and length(cspi.CSPI_ITS_PREFIX)=3 and mepe.MEPE_EFF_DT <= cspi.CSPI_TERM_DT and mepe.MEPE_TERM_DT >= cspi.CSPI_EFF_DT"+
         " where sbsb.sbsb_id=? and ? between cspi.CSPI_EFF_DT and cspi.CSPI_TERM_DT"+
         " 		    ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getIdAndBluesPrefixBySbsbIdAndDate" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (subscriberID) to " + subscriberID);
                
            ps.setString(parmNum++, subscriberID);
            
				log.debug("   Setting parm #" + parmNum + " (searchDate) to " + searchDate);
                

				if (searchDate == null) 
            {
                
                ps.setTimestamp(parmNum++, null);
                
            }
            else {
                
                ps.setTimestamp(parmNum++, new java.sql.Timestamp(searchDate.getTime()));
                
            }            
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberIdAndBluesPrefixDto dto = new FacetsMemberIdAndBluesPrefixDto();
                                
                dto.sbsbId = rs.getString("sbsb_id");
                if (rs.wasNull()) 
                {
                    dto.sbsbId = null;
                }
                                
                dto.prefix = rs.getString("CSPI_ITS_PREFIX");
                if (rs.wasNull()) 
                {
                    dto.prefix = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getIdAndBluesPrefixBySbsbIdAndDate  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get SRI code by SBSB_CK</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre> 
	            select SRI_CODE
	            from AGP.SRI 
				where SBSB_CK = ? AND ( term_dt is null OR (eff_dt is null and sysdate > term_dt) OR (sysdate between eff_dt and term_dt)  )
	        </pre></blockquote></p>
     * @param sbsbCK contrived key
     * @return A <tt>List</tt> of <tt>FacetsMemberSriDto</tt> objects that match the
     * selection criteria.  The <tt>List</tt> may be empty but will never return null.
     */
    public List<FacetsMemberSriDto> getSriBySbsbCK(String sbsbCK)
    {
        List<FacetsMemberSriDto> result = new ArrayList<FacetsMemberSriDto>();
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select SRI_CODE"+
         " from AGP.SRI"+
         " where SBSB_CK = ? AND ( term_dt is null OR (eff_dt is null and sysdate > term_dt) OR (sysdate between eff_dt and term_dt)  )"+
         " 	        ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getSriBySbsbCK" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (sbsbCK) to " + sbsbCK);
                
            ps.setString(parmNum++, sbsbCK);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberSriDto dto = new FacetsMemberSriDto();
                                
                dto.code = rs.getString("SRI_CODE");
                if (rs.wasNull()) 
                {
                    dto.code = null;
                }
                
                result.add(dto);
                
            }
            
            
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getSriBySbsbCK  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get hipaa</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre> SELECT SBSB.SBSB_ID, (CASE WHEN MAX(PMCC.PMCC_TERM_DTM) >= SYSDATE THEN 'Y' ELSE 'N' END) AS HIPAA
				FROM FHP_PMED_MEMBER_D PMED
				LEFT JOIN FHP_PMCC_COMM_X PMCC ON PMCC.PMED_CKE = PMED.PMED_CKE AND PMCC.PMCC_PZCD_STS = 'ACPT'
				INNER JOIN CMC_SBSB_SUBSC SBSB ON SBSB.SBSB_ID  = SUBSTR(PMED.PMED_ID,9,9)
				WHERE
				    SBSB.SBSB_ID = ?
				GROUP BY
				    SBSB.SBSB_ID</pre></blockquote></p>
     * @param memberId Amerigroup ID of the Facets member.
     * @return the <tt>FacetsMemberHippaDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberHippaDto getHipaa(String memberId)
    {
        FacetsMemberHippaDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = "SELECT SBSB.SBSB_ID, (CASE WHEN MAX(PMCC.PMCC_TERM_DTM) >= SYSDATE THEN 'Y' ELSE 'N' END) AS HIPAA"+
         " FROM FHP_PMED_MEMBER_D PMED"+
         " LEFT JOIN FHP_PMCC_COMM_X PMCC ON PMCC.PMED_CKE = PMED.PMED_CKE AND PMCC.PMCC_PZCD_STS = 'ACPT'"+
         " INNER JOIN CMC_SBSB_SUBSC SBSB ON SBSB.SBSB_ID  = SUBSTR(PMED.PMED_ID,9,9)"+
         " WHERE"+
         " SBSB.SBSB_ID = ?"+
         " GROUP BY"+
         " 				    SBSB.SBSB_ID";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getHipaa" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (memberId) to " + memberId);
                
            ps.setString(parmNum++, memberId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberHippaDto dto = new FacetsMemberHippaDto();
                                
                dto.amerigroupID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.amerigroupID = null;
                }
                                
                dto.hipaa = rs.getString("HIPAA");
                if (rs.wasNull()) 
                {
                    dto.hipaa = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getHipaa  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (m.MEME_MEDCD_NO =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityByMedicaidID(String medicaidId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK ,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityByMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityByMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (m.MEME_HICN =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicareId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityByCurrentMedicareID(String medicareId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_HICN =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityByCurrentMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityByCurrentMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (x.MECR_NO_ORIG = ?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicareId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityByOldMedicareID(String medicareId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y'"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (x.MECR_NO_ORIG = ?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityByOldMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityByOldMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (m.MEME_MEDCD_NO =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityWithNoEligibilityCheckByMedicaidID(String medicaidId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_MEDCD_NO =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityWithNoEligibilityCheckByMedicaidID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityWithNoEligibilityCheckByMedicaidID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicare id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (m.MEME_HICN =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicaidId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityWithNoEligibilityCheckByCurrentMedicareID(String medicaidId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (m.MEME_HICN =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityWithNoEligibilityCheckByCurrentMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicaidId) to " + medicaidId);
                
            ps.setString(parmNum++, medicaidId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityWithNoEligibilityCheckByCurrentMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibilities using the medicaid id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre>
				select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE
				from CMC_MEPE_PRCS_ELIG e
				inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK
				inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK
				left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK
				left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK
				left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK
				left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd
				left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id
				LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT
				left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id
				left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'
				where (x.MECR_NO_ORIG =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param medicareId Medicaid ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityWithNoEligibilityCheckByOldMedicareID(String medicareId)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = ""+
         " select m.MEME_FIRST_NAME, m.MEME_LAST_NAME, CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT, g.GRGR_MCTR_TYPE , e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK, g.GRGR_PHONE"+
         " from CMC_MEPE_PRCS_ELIG e"+
         " inner join CMC_MEME_MEMBER m on e.MEME_CK = m.MEME_CK"+
         " inner join CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK"+
         " left join CMC_MECR_NO_XREF x on x.MEME_CK = M.MEME_CK"+
         " left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK"+
         " left outer join CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK"+
         " left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd"+
         " left outer join CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id"+
         " LEFT OUTER JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT"+
         " left outer join CMC_PDDS_PROD_DESC pdesc  on e.pdpd_id = pdesc.pdpd_id"+
         " left outer join  cmc_mctr_cd_trans mctr on mctr.mctr_value = pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and mctr.MCTR_TYPE = 'VAL'"+
         " where (x.MECR_NO_ORIG =?) and TRUNC(SYSDATE) between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityWithNoEligibilityCheckByOldMedicareID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (medicareId) to " + medicareId);
                
            ps.setString(parmNum++, medicareId);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityWithNoEligibilityCheckByOldMedicareID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibility using the amerigroup id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre> select m.MEME_FIRST_NAME, m.MEME_LAST_NAME,
				CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME,
				m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT,
				e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC,
				g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME,
				p.GENERAL_DESC, e.CSPD_CAT, e.MEPE_ELIG_IND, g.GRGR_NAME,
				sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT,
				sg.SGSG_TERM_DT,g.GRGR_MCTR_TYPE, e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,
				mctr.mctr_desc, e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK,
				g.GRGR_PHONE from CMC_MEPE_PRCS_ELIG e inner join CMC_MEME_MEMBER m
				on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' inner join
				CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK left outer join
				CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK left outer join
				CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK left outer join
				AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and
				sg.sgsg_mctr_type=p.market_cd left outer join CMC_PLDS_PLAN_DESC
				pland ON e.cspi_id = pland.cspi_id LEFT OUTER JOIN
				FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and
				e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT left outer
				join CMC_PDDS_PROD_DESC pdesc on e.pdpd_id = pdesc.pdpd_id left
				outer join cmc_mctr_cd_trans mctr on mctr.mctr_value =
				pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and
				mctr.MCTR_TYPE = 'VAL' where (s.SBSB_ID =?) and TRUNC(SYSDATE)
				between e.MEPE_EFF_DT and e.MEPE_TERM_DT
			</pre></blockquote></p>
     * @param amerigroupID Amerigroup ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityByAmerigroupID(String amerigroupID)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = "select m.MEME_FIRST_NAME, m.MEME_LAST_NAME,"+
         " CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as FULL_LAST_NAME,"+
         " m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK, e.MEPE_EFF_DT,"+
         " e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID, pland.PLDS_DESC,"+
         " g.GRGR_ID, sg.SGSG_MCTR_TYPE, m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME,"+
         " p.GENERAL_DESC, e.CSPD_CAT, e.MEPE_ELIG_IND, g.GRGR_NAME,"+
         " sg.SGSG_NAME, sg.SGSG_ID, sg.SGSG_ORIG_EFF_DT,"+
         " sg.SGSG_TERM_DT,g.GRGR_MCTR_TYPE, e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,"+
         " mctr.mctr_desc, e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK,"+
         " g.GRGR_PHONE from CMC_MEPE_PRCS_ELIG e inner join CMC_MEME_MEMBER m"+
         " on e.MEME_CK = m.MEME_CK and e.MEPE_ELIG_IND = 'Y' inner join"+
         " CMC_SBSB_SUBSC s on m.SBSB_CK = s.SBSB_CK left outer join"+
         " CMC_SGSG_SUB_GROUP sg on e.SGSG_CK = sg.SGSG_CK left outer join"+
         " CMC_GRGR_GROUP g on g.GRGR_CK = sg.GRGR_CK left outer join"+
         " AGP.CCTR_PRODUCT p on e.PDPD_ID = p.PRODUCT_ID and"+
         " sg.sgsg_mctr_type=p.market_cd left outer join CMC_PLDS_PLAN_DESC"+
         " pland ON e.cspi_id = pland.cspi_id LEFT OUTER JOIN"+
         " FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and"+
         " e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT left outer"+
         " join CMC_PDDS_PROD_DESC pdesc on e.pdpd_id = pdesc.pdpd_id left"+
         " outer join cmc_mctr_cd_trans mctr on mctr.mctr_value ="+
         " pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and"+
         " mctr.MCTR_TYPE = 'VAL' where (s.SBSB_ID =?) and TRUNC(SYSDATE)"+
         " between e.MEPE_EFF_DT and e.MEPE_TERM_DT"+
         " 			";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityByAmerigroupID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (amerigroupID) to " + amerigroupID);
                
            ps.setString(parmNum++, amerigroupID);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityByAmerigroupID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

		
    /**
     * <p>Get a members current eligibility using the amerigroup id</p>
     * <p><b>Not transaction-aware.</b></p>
     * <p>Executes the following SQL:<br/>
     * <blockquote><pre> select m.MEME_FIRST_NAME,
				m.MEME_LAST_NAME,CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as
				FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK,
				e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID,
				pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE,
				m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,
				e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID,
				sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT,g.GRGR_MCTR_TYPE,
				e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,
				mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK,
				g.GRGR_PHONE from CMC_MEPE_PRCS_ELIG e inner join CMC_MEME_MEMBER m
				on e.MEME_CK = m.MEME_CK inner join CMC_SBSB_SUBSC s on m.SBSB_CK =
				s.SBSB_CK left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK =
				sg.SGSG_CK left outer join CMC_GRGR_GROUP g on g.GRGR_CK =
				sg.GRGR_CK left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID =
				p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd left outer join
				CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id LEFT OUTER
				JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and
				e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT left outer
				join CMC_PDDS_PROD_DESC pdesc on e.pdpd_id = pdesc.pdpd_id left
				outer join cmc_mctr_cd_trans mctr on mctr.mctr_value =
				pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and
				mctr.MCTR_TYPE = 'VAL' where (s.SBSB_ID =?) and TRUNC(SYSDATE)
				between e.MEPE_EFF_DT and e.MEPE_TERM_DT </pre></blockquote></p>
     * @param amerigroupID Amerigroup ID of the member
     * @return the <tt>FacetsMemberEligibilityDto</tt> object that matches the
     * selection criteria, or null if there is no match.
     */
    public FacetsMemberEligibilityDto getCurrentEligibilityWithNoEligibilityCheckByAmerigroupID(String amerigroupID)
    {
        FacetsMemberEligibilityDto result = null;
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        String sql = "select m.MEME_FIRST_NAME,"+
         " m.MEME_LAST_NAME,CONCAT(m.MEME_LAST_NAME, m.MEME_TITLE) as"+
         " FULL_LAST_NAME, m.MEME_BIRTH_DT, pd.LOBD_ID, s.SBSB_ID, s.SBSB_CK,"+
         " e.MEPE_EFF_DT, e.MEPE_TERM_DT, e.CSCS_ID, e.PDPD_ID, e.CSPI_ID,"+
         " pland.PLDS_DESC, g.GRGR_ID, sg.SGSG_MCTR_TYPE,"+
         " m.MEME_MEDCD_NO,m.MEME_HICN, p.NAME, p.GENERAL_DESC, e.CSPD_CAT,"+
         " e.MEPE_ELIG_IND, g.GRGR_NAME, sg.SGSG_NAME, sg.SGSG_ID,"+
         " sg.SGSG_ORIG_EFF_DT, sg.SGSG_TERM_DT,g.GRGR_MCTR_TYPE,"+
         " e.GRGR_CK,pdesc.PDDS_MCTR_VAL1,"+
         " mctr.mctr_desc,e.MEPE_PLAN_ENTRY_DT,e.MEPE_CREATE_DTM,m.MEME_CK,"+
         " g.GRGR_PHONE from CMC_MEPE_PRCS_ELIG e inner join CMC_MEME_MEMBER m"+
         " on e.MEME_CK = m.MEME_CK inner join CMC_SBSB_SUBSC s on m.SBSB_CK ="+
         " s.SBSB_CK left outer join CMC_SGSG_SUB_GROUP sg on e.SGSG_CK ="+
         " sg.SGSG_CK left outer join CMC_GRGR_GROUP g on g.GRGR_CK ="+
         " sg.GRGR_CK left outer join AGP.CCTR_PRODUCT p on e.PDPD_ID ="+
         " p.PRODUCT_ID and sg.sgsg_mctr_type=p.market_cd left outer join"+
         " CMC_PLDS_PLAN_DESC pland ON e.cspi_id = pland.cspi_id LEFT OUTER"+
         " JOIN FACETS.CMC_PDPD_PRODUCT pd on e.PDPD_ID = pd.PDPD_ID and"+
         " e.MEPE_EFF_DT between pd.PDPD_EFF_DT and pd.PDPD_TERM_DT left outer"+
         " join CMC_PDDS_PROD_DESC pdesc on e.pdpd_id = pdesc.pdpd_id left"+
         " outer join cmc_mctr_cd_trans mctr on mctr.mctr_value ="+
         " pdesc.PDDS_MCTR_VAL1 and mctr.mctr_entity = 'PDDS' and"+
         " mctr.MCTR_TYPE = 'VAL' where (s.SBSB_ID =?) and TRUNC(SYSDATE)"+
         " 				between e.MEPE_EFF_DT and e.MEPE_TERM_DT ";
        
        try 
        {
	        long connectionStart = System.currentTimeMillis();
			conn = getConnection();
			if (conn == null) {
				log.error("Unable to get a connection to datasource " + getDatasourceJndiName());
					log.debug("<getCurrentEligibilityWithNoEligibilityCheckByAmerigroupID" );
				return result;
			}

				StringBuilder sb = null;
            
            ps = conn.prepareStatement(sql);
            
            long connectionDuration = System.currentTimeMillis() - connectionStart;
            
            int parmNum = 1;
            
				log.debug("Executing SQL: " + sql);
            
				log.debug("   Setting parm #" + parmNum + " (amerigroupID) to " + amerigroupID);
                
            ps.setString(parmNum++, amerigroupID);
            
            long queryStart = System.currentTimeMillis();
				log.debug("Starting query");
            rs = ps.executeQuery();
            int queryDuration = (int) (System.currentTimeMillis() - queryStart);

            // Load results into list            
            long loadResultsStart = System.currentTimeMillis();
            int rowsRead = 0;
            while (rs.next())
            {
            	rowsRead++;
                FacetsMemberEligibilityDto dto = new FacetsMemberEligibilityDto();
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                                
                dto.groupCk = rs.getString("GRGR_CK");
                if (rs.wasNull()) 
                {
                    dto.groupCk = null;
                }
                                
                dto.sbsbID = rs.getString("SBSB_ID");
                if (rs.wasNull()) 
                {
                    dto.sbsbID = null;
                }
                                
                dto.sbsbCK = rs.getString("SBSB_CK");
                if (rs.wasNull()) 
                {
                    dto.sbsbCK = null;
                }
                                
                dto.classID = rs.getString("CSCS_ID");
                if (rs.wasNull()) 
                {
                    dto.classID = null;
                }
                                
                dto.planID = rs.getString("CSPI_ID");
                if (rs.wasNull()) 
                {
                    dto.planID = null;
                }
                                
                dto.planDesc = rs.getString("PLDS_DESC");
                if (rs.wasNull()) 
                {
                    dto.planDesc = null;
                }
                                
                dto.groupID = rs.getString("GRGR_ID");
                if (rs.wasNull()) 
                {
                    dto.groupID = null;
                }
                                
                dto.subgroupType = rs.getString("SGSG_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.subgroupType = null;
                }
                                
                dto.subGroupId = rs.getString("SGSG_ID");
                if (rs.wasNull()) 
                {
                    dto.subGroupId = null;
                }
                                
                dto.medicaidID = rs.getString("MEME_MEDCD_NO");
                if (rs.wasNull()) 
                {
                    dto.medicaidID = null;
                }
                                
                dto.medicareID = rs.getString("MEME_HICN");
                if (rs.wasNull()) 
                {
                    dto.medicareID = null;
                }
                                
                dto.productID = rs.getString("PDPD_ID");
                if (rs.wasNull()) 
                {
                    dto.productID = null;
                }
                                
                dto.productName = rs.getString("NAME");
                if (rs.wasNull()) 
                {
                    dto.productName = null;
                }
                                
                dto.productDescription = rs.getString("GENERAL_DESC");
                if (rs.wasNull()) 
                {
                    dto.productDescription = null;
                }
                                
                dto.productValueCode = rs.getString("PDDS_MCTR_VAL1");
                if (rs.wasNull()) 
                {
                    dto.productValueCode = null;
                }
                                
                dto.productValueCodeDesc = rs.getString("MCTR_DESC");
                if (rs.wasNull()) 
                {
                    dto.productValueCodeDesc = null;
                }
                
                dto.dateEffective = rs.getTimestamp("MEPE_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.dateEffective = null;
                }
                
                dto.dateTermination = rs.getTimestamp("MEPE_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.dateTermination = null;
                }
                                
                dto.coverageTypeCode = rs.getString("CSPD_CAT");
                if (rs.wasNull()) 
                {
                    dto.coverageTypeCode = null;
                }
                                
                dto.firstName = rs.getString("MEME_FIRST_NAME");
                if (rs.wasNull()) 
                {
                    dto.firstName = null;
                }
                                
                dto.lastName = rs.getString("MEME_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.lastName = null;
                }
                                
                dto.fullLastName = rs.getString("FULL_LAST_NAME");
                if (rs.wasNull()) 
                {
                    dto.fullLastName = null;
                }
                
                dto.birthDt = rs.getTimestamp("MEME_BIRTH_DT");
                if (rs.wasNull()) 
                {
                    dto.birthDt = null;
                }
                                
                dto.lobId = rs.getString("LOBD_ID");
                if (rs.wasNull()) 
                {
                    dto.lobId = null;
                }
                                
                dto.statusCode = rs.getString("MEPE_ELIG_IND");
                if (rs.wasNull()) 
                {
                    dto.statusCode = null;
                }
                                
                dto.groupName = rs.getString("GRGR_NAME");
                if (rs.wasNull()) 
                {
                    dto.groupName = null;
                }
                                
                dto.subGroupName = rs.getString("SGSG_NAME");
                if (rs.wasNull()) 
                {
                    dto.subGroupName = null;
                }
                
                dto.subGroupEffectiveDate = rs.getTimestamp("SGSG_ORIG_EFF_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupEffectiveDate = null;
                }
                
                dto.subGroupTerminationDate = rs.getTimestamp("SGSG_TERM_DT");
                if (rs.wasNull()) 
                {
                    dto.subGroupTerminationDate = null;
                }
                                
                dto.sourceSystem = rs.getString("GRGR_MCTR_TYPE");
                if (rs.wasNull()) 
                {
                    dto.sourceSystem = null;
                }
                
                dto.planEntryDt = rs.getTimestamp("MEPE_PLAN_ENTRY_DT");
                if (rs.wasNull()) 
                {
                    dto.planEntryDt = null;
                }
                
                dto.eligibilityCreateDtm = rs.getTimestamp("MEPE_CREATE_DTM");
                if (rs.wasNull()) 
                {
                    dto.eligibilityCreateDtm = null;
                }
                                
                dto.memeCk = rs.getString("MEME_CK");
                if (rs.wasNull()) 
                {
                    dto.memeCk = null;
                }
                                
                dto.grpPhNo = rs.getString("GRGR_PHONE");
                if (rs.wasNull()) 
                {
                    dto.grpPhNo = null;
                }
                
                result = dto;
                break;
                
            }
            
            
            if (rs.next()) 
            {
                log.warn("Warning - SQL query returned multiple results but DAO cardinality was not 'many' - possible ambiguous results."); 
            }
             
            long loadResultsDuration = System.currentTimeMillis() - loadResultsStart;
            log.info(String.format("Query Statistics:getCurrentEligibilityWithNoEligibilityCheckByAmerigroupID  getJndiName="+getDatasourceJndiName()+", getConnection=%s, queryExecution=%s, loadResults=%s, rowsRead=%s", connectionDuration, queryDuration, loadResultsDuration, rowsRead));
        }
        catch (SQLException ex) 
        {
            SQLException tex = ex;
            if (ex.getNextException() != null)
            {
                tex = ex.getNextException();
            }
            while (tex != null)
            {
                log.error("Database problem: " + tex.getMessage()
                        + " - error code=" + tex.getErrorCode() + ", SQLState="
                        + tex.getSQLState() + " - SQL: " + sql, tex);
                tex = tex.getNextException();
            }
            throw new DAOException("Unable to perform read - SQL: " + sql, ex);
        }
        finally
        {
            closeDatabaseObjects(rs, ps, conn);
        }
        return result;
    }
	

    
}
// CHECKSTYLE:ON
/* END OF FILE  - com.amerigroup.facets.dao.FacetsMemberDaoImpl */
