package gov.va.umls;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ValuePropertyPair;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.BaseConverter;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import gov.va.umls.propertyTypes.PT_Attributes;
import gov.va.umls.propertyTypes.PT_IDs;
import gov.va.umls.rrf.MRCONSO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.maven.plugin.MojoExecutionException;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.description.TkDescription;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;

/**
 * Goal to convert UMLS data
 * 
 * @goal buildUMLS
 * 
 * @phase process-sources
 */
public class UMLSMojo extends BaseConverter 
{
	private PropertyType ptSTT_Types_;
	private PreparedStatement satAtomStatement, satConceptStatement, semanticTypeStatement, 
		cuiRelStatementForward, auiRelStatementForward, cuiRelStatementBackward, auiRelStatementBackward,
		definitionStatement;
	
	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	
	/**
	 * Where UMLS source files are
	 * 
	 * @parameter
	 * @required
	 */
	protected File srcDataPath;
	
	/**
	 * Where to write the H2 database
	 * 
	 * @parameter
	 * @optional
	 */
	protected File tmpDBPath;

	/**
	 * Location of the file.
	 * 
	 * @parameter expression="${project.build.directory}"
	 * @required
	 */
	protected File outputDirectory;
	
	/**
	 * Loader version number
	 * Use parent because project.version pulls in the version of the data file, which I don't want.
	 * 
	 * @parameter expression="${project.parent.version}"
	 * @required
	 */
	protected String loaderVersion;

	/**
	 * Content version number
	 * 
	 * @parameter expression="${project.version}"
	 * @required
	 */
	protected String releaseVersion;
	
	/**
	 * A list of SABs to include in the load.  If provided, any SABs that do not match are excluded from the conversion.
	 * If not provided, or blank, all data found in the Metamorphosys output it loaded.
	 * 
	 * @parameter
	 * @optional
	 */
	private List<String> sabFilters;

	public void execute() throws MojoExecutionException
	{
		long startTime = System.currentTimeMillis();
		ConsoleUtil.println(new Date().toString());
		
		try
		{
			outputDirectory.mkdir();
			
			if (srcDataPath.isDirectory())
			{
				if (!new File(srcDataPath, "META").isDirectory())
				{
					throw new MojoExecutionException("Please configure UMLS-eConcept/pom.xml 'srcDataPath' to point to the version output folder of metamorphosys."
							+ "  Didn't find the expected 'META' folder under " + srcDataPath.getAbsolutePath());
				}
			}
			else
			{
				throw new MojoExecutionException("Please configure UMLS-eConcept/pom.xml 'srcDataPath' to point to the version output folder of metamorphosys."
						+ "  Currently set to " + srcDataPath.getAbsolutePath());
			}

			loadDatabase();
			
			init(outputDirectory, "UMLS", "MR", new PT_IDs(), new PT_Attributes(), sabFilters);
			
			satAtomStatement = db_.getConnection().prepareStatement("select * from MRSAT where CUI = ? and METAUI = ? " + (sabQueryString_.length() > 0 ? "and " + sabQueryString_ : ""));
			satConceptStatement = db_.getConnection().prepareStatement("select * from MRSAT where CUI = ? and METAUI is null " + (sabQueryString_.length() > 0 ? "and " + sabQueryString_ : ""));
			semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from MRSTY where CUI = ?");
			definitionStatement = db_.getConnection().prepareStatement("select * from MRDEF where CUI = ? and AUI = ?");
			
			//UMLS and RXNORM do different things with rels - UMLS never has null CUI's, while RxNorm always has null CUI's (when AUI is specified)
			//Also need to join back to MRCONSO for the cases where we are applying a SAB filter, to make sure both the source and the target are things that will be loaded.
			
			String sabQueryString1 = sabQueryString_.replaceAll("SAB", "r.SAB");
			String sabQueryString2 = sabQueryString_.replaceAll("SAB", "MRCONSO.SAB");
			
			cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT r.CUI1, r.AUI1, r.STYPE1, r.REL, r.CUI2, r.AUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from MRREL as r"  
					+ (sabQueryString_.length() > 0 ? ", MRCONSO" : "")
					+ " WHERE CUI2 = ? and AUI2 is null " 
					+ (sabQueryString_.length() > 0 ? "and " + sabQueryString1 + "and r.CUI1 = MRCONSO.CUI and " + sabQueryString2: ""));
			
			cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT r.CUI1, r.AUI1, r.STYPE1, r.REL, r.CUI2, r.AUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from MRREL as r"  
					+ (sabQueryString_.length() > 0 ? ", MRCONSO" : "")
					+ " WHERE CUI1 = ? and AUI1 is null " 
					+ (sabQueryString_.length() > 0 ? "and " + sabQueryString1 + "and r.CUI2 = MRCONSO.CUI and " + sabQueryString2: ""));

			auiRelStatementForward = db_.getConnection().prepareStatement("SELECT r.CUI1, r.AUI1, r.STYPE1, r.REL, r.CUI2, r.AUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from MRREL as r"  
					+ (sabQueryString_.length() > 0 ? ", MRCONSO" : "")
					+ " WHERE CUI2 = ? and AUI2 = ? " 
					+ (sabQueryString_.length() > 0 ? "and " + sabQueryString1 + "and r.CUI1 = MRCONSO.CUI and r.AUI1 = MRCONSO.AUI and " + sabQueryString2: ""));
			
			auiRelStatementBackward = db_.getConnection().prepareStatement("SELECT r.CUI1, r.AUI1, r.STYPE1, r.REL, r.CUI2, r.AUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from MRREL as r"  
					+ (sabQueryString_.length() > 0 ? ", MRCONSO" : "")
					+ " WHERE CUI1 = ? and AUI1 = ? " 
					+ (sabQueryString_.length() > 0 ? "and " + sabQueryString1 + "and r.CUI2 = MRCONSO.CUI and r.AUI2 = MRCONSO.AUI and " + sabQueryString2: ""));
			
			allRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.ALL.getSourcePropertyNameFSN());
			allCUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.CUI_CONCEPTS.getSourcePropertyNameFSN());
			allAUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.AUI_CONCEPTS.getSourcePropertyNameFSN());
			
			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), false);
			eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, ptContentVersion_.RELEASE.getUUID(), false);
			
			//Disable the masterUUID debug map now that the metadata is populated, not enough memory on most systems to maintain it for everything else.
			//ConverterUUID.disableUUIDMap_ = true;
			
			//process
			int cuiCounter = 0;
			
			Statement statement = db_.getConnection().createStatement();
			ResultSet rs = statement.executeQuery("select * from MRCONSO " + (sabQueryString_.length() > 0 ? " where " + sabQueryString_ : "") + " order by CUI");
			ArrayList<MRCONSO> conceptData = new ArrayList<>();
			while (rs.next())
			{
				MRCONSO current = new MRCONSO(rs);
				if (conceptData.size() > 0 && !conceptData.get(0).cui.equals(current.cui))
				{
					processCUIRows(conceptData);
					if (cuiCounter % 10 == 0)
					{
						ConsoleUtil.showProgress();
					}
					cuiCounter++;
					if (cuiCounter % 10000 == 0)
					{
						ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
					}
					conceptData.clear();
				}
				conceptData.add(current);
			}
			rs.close();
			statement.close();

			
			// process last
			processCUIRows(conceptData);
			
			satAtomStatement.close();
			satConceptStatement.close();
			semanticTypeStatement.close();
			definitionStatement.close();
			cuiRelStatementForward.close();
			cuiRelStatementBackward.close();
			auiRelStatementBackward.close();
			auiRelStatementForward.close();
			
			finish(outputDirectory);
			
			db_.shutdown();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new MojoExecutionException("Failure during export ", e);
		}
		finally
		{
			try
			{
				db_.shutdown();
			}
			catch (SQLException e)
			{
				ConsoleUtil.printErrorln("Error closing source DB: " + e);
			}
		}
		
		ConsoleUtil.println("Completed in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds");
		ConsoleUtil.println(new Date().toString());
		
	}

	private void loadDatabase() throws Exception
	{
		// Set up the DB for loading the temp data
		db_ = new RRFDatabaseHandle();
		File meta = new File(srcDataPath, "META");
		
		File h2Folder;
		
		if (tmpDBPath != null && tmpDBPath.isDirectory())
		{
			h2Folder = tmpDBPath;
		}
		else
		{
			h2Folder = outputDirectory;
		}
		
		File dbFile = new File(h2Folder, "umlsRRF_DB.h2.db");
		boolean createdNew = db_.createOrOpenDatabase(new File(h2Folder, "umlsRRF_DB"));

		if (!createdNew)
		{
			ConsoleUtil.println("Using existing database.  To load from scratch, delete the file '" + dbFile.getAbsolutePath() + ".*'");
		}
		else
		{
			HashSet<String> filesToSkip = new HashSet<>();
			filesToSkip.add("MRHIST.RRF");  //snomed history - already in the WB.
			filesToSkip.add("MRMAP.RRF");  //not doing mappings yet
			filesToSkip.add("MRSMAP.RRF");  //not doing mappings yet
			filesToSkip.add("CHANGE/*");  //not loading these yet
			filesToSkip.add("MRAUI.RRF");  //don't need - historical info
			filesToSkip.add("MRCUI.RRF");  //retired CUIs
			filesToSkip.add("MRX*");  //indexes - don't need
 			
			List<TableDefinition> tables = db_.loadTableDefinitionsFromMRCOLS(new FileInputStream(new File(meta, "MRFILES.RRF")), 
					new FileInputStream(new File(meta, "MRCOLS.RRF")), filesToSkip);

			for (TableDefinition td : tables)
			{

				String tableName = td.getTableName();
				File dataFile;
				if (tableName.indexOf('/') > 0)
				{
					String dir = tableName.substring(0, tableName.indexOf('/'));
					String name = tableName.substring(tableName.indexOf('/') + 1, tableName.length());
					dataFile = new File(new File(meta, dir), name + ".RRF");
				}
				else
				{
					dataFile = new File(meta, tableName + ".RRF");
				}
				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new FileReader(dataFile))), sabFilters);
			}

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_cui_index ON MRCONSO (CUI, AUI)");  //make order by fast - also used in adding rels 
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_cui_metaui_index ON MRSAT (CUI, METAUI)");  // concept/atom sat lookup
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_metaui_index ON MRSAT (METAUI)");  //rel sat lookup
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_cui_index ON MRSTY (CUI)");  //semantic type lookup
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON MRSTY (TUI)");  //select distinct tui during metadata
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_2_index ON MRREL (CUI2, AUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_1_index ON MRREL (CUI1, AUI1)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_index ON MRREL (RELA)");  //helps with rel metadata
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX paui_index ON MRHIER (PAUI)");  //for looking up if a term has roots
			s.close();
		}
	}
	@Override
	protected void loadCustomMetaData() throws Exception
	{
		//STT Types
		ptSTT_Types_= new PropertyType("STT Types"){};
		{
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT DISTINCT VALUE, TYPE, EXPL FROM MRDOC where DOCKEY ='STT'");
			while (rs.next())
			{
				String stt = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expl = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}				
				
				ptSTT_Types_.addProperty(stt, expl, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSTT_Types_, metaDataRoot_, dos_);
	}
	
	//TODO load the rest of the MRDOC types as metadata, use as UUID annotations instead of STR...
	
	@Override
	protected void addCustomRefsets(BPT_Refsets refset) throws Exception
	{
		//noop
	}
	//TODO nested attributes on SCUI, SLUI, code?  Take another look at how I query the SAT file.
	@Override
	protected void processSAT(TkComponent<?> itemToAnnotate, ResultSet rs) throws SQLException
	{
		while (rs.next())
		{
			//String cui = rs.getString("CUI");
			String lui = rs.getString("LUI");
			String sui = rs.getString("SUI");
			//String metaui = rs.getString("METAUI");
			String stype = rs.getString("STYPE");
			String code = rs.getString("CODE");
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String atn = rs.getString("ATN");
			String sab = rs.getString("SAB");
			String atv = rs.getString("ATV");
			String suppress = rs.getString("SUPPRESS");
			Integer cvf = rs.getObject("CVF") == null ? null : rs.getInt("CVF");

			TkRefsetStrMember attribute = eConcepts_.addStringAnnotation(itemToAnnotate, ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui), atv, 
					ptTermAttributes_.get(sab).getProperty(atn).getUUID(), false, null);
			
			if (lui != null)
			{
				eConcepts_.addStringAnnotation(attribute, lui, ptUMLSAttributes_.getProperty("LUI").getUUID(), false);
			}
			
			if (sui != null)
			{
				eConcepts_.addStringAnnotation(attribute, sui, ptUMLSAttributes_.getProperty("SUI").getUUID(), false);
			}
			
			if (stype != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptUMLSAttributes_.getProperty("STYPE").getUUID());
			}
			
			if (code != null)
			{
				eConcepts_.addStringAnnotation(attribute, code, ptUMLSAttributes_.getProperty("CODE").getUUID(), false);
			}
			
			if (atui != null)
			{
				//TODO should this be an ID?
				eConcepts_.addStringAnnotation(attribute, atui, ptUMLSAttributes_.getProperty("ATUI").getUUID(), false);
			}
			
			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptUMLSAttributes_.getProperty("SATUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());
			
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				eConcepts_.addStringAnnotation(attribute, cvf.toString(), ptUMLSAttributes_.getProperty("CVF").getUUID(), false);
			}
		}
		rs.close();
	}

	@Override
	protected Property makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes)
	{
		//Will fill in the rankings below, in the allDescriptionsCreated method
		return new Property(null, fsnName, preferredName, null, false, 0);
	}


	@Override
	protected void allDescriptionsCreated(String sab) throws SQLException
	{
		/**
		 * Run through and set all of the description type ranking info.  Bump FN to the FSN category - other than that, 
		 * let the MRRANK file specify the order - putting most things in as synonyms and adding one each time we go down the rank rows.
		 */
		int fsnPos = BPT_Descriptions.FSN;
		int synonymPos = BPT_Descriptions.SYNONYM;
		
		Statement s = db_.getConnection().createStatement();
		ResultSet rs = s.executeQuery("select RANK, TTY from MRRANK WHERE sab='" + sab + "' order by RANK desc");
		int resultCount = 0;
		while (rs.next())
		{
			resultCount++;
			String tty = rs.getString("TTY");
			
			Property p = ptDescriptions_.get(sab).getProperty(tty);
			if (p.getSourcePropertyNameFSN().equals("FN"))
			{
				p.setPropertySubType(fsnPos++);
			}
			else
			{
				p.setPropertySubType(synonymPos++);
			}
		}
		if (synonymPos >= BPT_Descriptions.DEFINITION)
		{
			throw new RuntimeException("OOPS - need to increase allowed synonym limit");
		}
		if (ptDescriptions_.get(sab).getProperties().size() > resultCount)
		{
			throw new RuntimeException("Not enough ranking data!");
		}
		rs.close();
		s.close();
	}
	
	private void processCUIRows(ArrayList<MRCONSO> conceptData) throws IOException, SQLException
	{
		EConcept cuiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("CUI" + conceptData.get(0).cui, true));
		eConcepts_.addAdditionalIds(cuiConcept, conceptData.get(0).cui, ptIds_.getProperty("CUI").getUUID(), false);

		ArrayList<ValuePropertyPair> descriptions = new ArrayList<>();
		
		for (MRCONSO consoRowData : conceptData)
		{
			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("AUI" + consoRowData.aui, true));
			eConcepts_.addAdditionalIds(auiConcept, consoRowData.aui, ptIds_.getProperty("AUI").getUUID(), false);
			
			// TODO handle language.
			if (!consoRowData.lat.equals("ENG"))
			{
				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
			}
			
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.ts, ptUMLSAttributes_.getProperty("TS").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.lui, ptUMLSAttributes_.getProperty("LUI").getUUID(), false);
			eConcepts_.addUuidAnnotation(auiConcept, ptSTT_Types_.getProperty(consoRowData.stt).getUUID(), ptUMLSAttributes_.getProperty("STT").getUUID());
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.sui, ptUMLSAttributes_.getProperty("SUI").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.ispref, ptUMLSAttributes_.getProperty("ISPREF").getUUID(), false);

			
			if (consoRowData.saui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.saui, ptUMLSAttributes_.getProperty("SAUI").getUUID(), false);
			}
			if (consoRowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.scui, ptUMLSAttributes_.getProperty("SCUI").getUUID(), false);
			}
			if (consoRowData.sdui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.sdui, ptUMLSAttributes_.getProperty("SDUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSABs_.getProperty(consoRowData.sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());

			if (consoRowData.code != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.code, ptUMLSAttributes_.getProperty("CODE").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSourceRestrictionLevels_.getProperty(consoRowData.srl.toString()).getUUID(), ptUMLSAttributes_.getProperty("SRL")
					.getUUID());

			eConcepts_.addUuidAnnotation(auiConcept, ptSuppress_.getProperty(consoRowData.suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS")
					.getUUID());
			
			if (consoRowData.cvf != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.cvf.toString(), ptUMLSAttributes_.getProperty("CVF").getUUID(), false);
			}
			
			
			eConcepts_.addDescription(auiConcept, consoRowData.str, DescriptionType.FSN, true, ptDescriptions_.get(consoRowData.sab).getProperty(consoRowData.tty).getUUID(), 
					ptDescriptions_.get(consoRowData.sab).getPropertyTypeReferenceSetUUID(), false);
			
			//used for sorting description to find one for the CUI concept
			descriptions.add(new ValuePropertyPair(consoRowData.str, ptDescriptions_.get(consoRowData.sab).getProperty(consoRowData.tty)));
			
			//Add Atom attributes
			satAtomStatement.clearParameters();
			satAtomStatement.setString(1, consoRowData.cui);
			satAtomStatement.setString(2, consoRowData.aui);
			ResultSet rs = satAtomStatement.executeQuery();
			processSAT(auiConcept.getConceptAttributes(), rs);
			
			//Add Definitions
			addDefinitions(auiConcept, consoRowData.cui, consoRowData.aui);
			
			//Add rel to parent CUI
			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
			
			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(ptRefsets_.get(consoRowData.sab).getConcept(terminologyAUIRefsetPropertyName_) , auiConcept.getPrimordialUuid(), null, true, null);
			
			auiRelStatementForward.clearParameters();
			auiRelStatementForward.setString(1, consoRowData.cui);
			auiRelStatementForward.setString(2, consoRowData.aui);
			addRelationships(auiConcept, auiRelStatementForward.executeQuery(), true);
			
			auiRelStatementBackward.clearParameters();
			auiRelStatementBackward.setString(1, consoRowData.cui);
			auiRelStatementBackward.setString(2, consoRowData.aui);
			addRelationships(auiConcept, auiRelStatementBackward.executeQuery(), false);
			
			//If root concept, add rel to UMLS root concept
			if (isRootConcept(consoRowData.cui, consoRowData.aui))
			{
				eConcepts_.addRelationship(auiConcept, umlsRootConcept_);
			}
			
			auiConcept.writeExternal(dos_);
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(descriptions);
		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
				descriptions.get(0).getProperty().getPropertyType().getPropertyTypeReferenceSetUUID(), false);
		
		//process concept attributes
		satConceptStatement.clearParameters();
		satConceptStatement.setString(1, conceptData.get(0).cui);
		ResultSet rs = satConceptStatement.executeQuery();
		processSAT(cuiConcept.getConceptAttributes(), rs);
		
		//add semantic types
		semanticTypeStatement.clearParameters();
		semanticTypeStatement.setString(1, conceptData.get(0).cui);
		rs = semanticTypeStatement.executeQuery();
		processSemanticTypes(cuiConcept, rs);

		cuiRelStatementForward.clearParameters();
		cuiRelStatementForward.setString(1, conceptData.get(0).cui);
		addRelationships(cuiConcept, cuiRelStatementForward.executeQuery(), true);
		
		cuiRelStatementBackward.clearParameters();
		cuiRelStatementBackward.setString(1, conceptData.get(0).cui);
		addRelationships(cuiConcept, cuiRelStatementBackward.executeQuery(), false);

		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		cuiConcept.writeExternal(dos_);
	}
	
	private void addDefinitions(EConcept concept, String cui, String aui) throws SQLException
	{
		definitionStatement.clearParameters();
		definitionStatement.setString(1, cui);
		definitionStatement.setString(2, aui);
		ResultSet rs = definitionStatement.executeQuery();
		while (rs.next())
		{
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String sab = rs.getString("SAB");
			String def = rs.getString("DEF");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getString("CVF");
			
			TkDescription d = eConcepts_.addDescription(concept, ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui, false), 
					def, DescriptionType.DEFINITION, false, null, null, false);
			
			//TODO ID or string?
			eConcepts_.addAdditionalIds(d, atui, ptIds_.getProperty("ATUI").getUUID());
			
			if (satui != null)
			{
				eConcepts_.addStringAnnotation(d, satui, ptUMLSAttributes_.getProperty("SATUI").getUUID(), false);
			}
			eConcepts_.addUuidAnnotation(d, ptSABs_.getProperty(sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());
			
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(d, ptSuppress_.getProperty(suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				eConcepts_.addStringAnnotation(d, cvf.toString(), ptUMLSAttributes_.getProperty("CVF").getUUID(), false);
			}
		}
	}


	public static void main(String[] args) throws MojoExecutionException
	{
		UMLSMojo mojo = new UMLSMojo();
		mojo.outputDirectory = new File("../UMLS-econcept/target");
		mojo.loaderVersion = "eclipse debug loader";
		mojo.releaseVersion = "eclipse debug release";
		//mojo.srcDataPath = new File("/mnt/d/Work/Apelon/UMLS/extracted/2013AA/");
		mojo.srcDataPath = new File("/mnt/d/Work/Apelon/UMLS/extracted-small/2013AA/");
		//mojo.tmpDBPath = new File("/mnt/d/Scratch/");
		//mojo.sabFilters = new ArrayList<String>();
		//mojo.SABFilterList.add("CCS");
		mojo.execute();
	}
}
