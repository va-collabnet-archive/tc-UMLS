package gov.va.umls;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal to build RxNorm
 * 
 * @goal buildRxNorm
 * 
 * @phase process-sources
 */
public class UMLSMojo extends AbstractMojo
{
	private RRFDatabaseHandle db_;
//	private HashMap<String, String> loadedRels_ = new HashMap<>();
//	private HashMap<String, String> skippedRels_ = new HashMap<>();
//
//	private EConcept allRefsetConcept_;
//	private EConcept allCUIRefsetConcept_;
//	private EConcept allAUIRefsetConcept_;
//	private EConcept cpcRefsetConcept_;

	/**
	 * Where UMLS source files are
	 * 
	 * @parameter
	 * @required
	 */
	private File srcDataPath;
	
	/**
	 * Where to write the H2 database
	 * 
	 * @parameter
	 * @optional
	 */
	private File tmpDBPath;

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
	private String loaderVersion;

	/**
	 * Content version number
	 * 
	 * @parameter expression="${project.version}"
	 * @required
	 */
	private String releaseVersion;
	
	/**
	 * A list of SABs to include in the load.  If provided, any SABs that do not match are excluded from the conversion.
	 * If not provided, or blank, all data found in the Metamorphosys output it loaded.
	 * 
	 * @optional
	 */
	private HashSet<String> SABFilterList;

	public void execute() throws MojoExecutionException
	{
		long startTime = System.currentTimeMillis();
		ConsoleUtil.println(new Date().toString());
		
		try
		{
			outputDirectory.mkdir();
			SABLoader.clearTargetFiles(outputDirectory);
			
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
			
			if (SABFilterList == null || SABFilterList.size() == 0)
			{
				//TODO query - find all SABs in provided data set.
			}
			
			for (String sab : SABFilterList)
			{
				//TODO get SSN from MRSAB
				SABLoader sl = new SABLoader(sab, sab, outputDirectory, db_);
			}

//			allRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.ALL.getProperty());
//			cpcRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CPC.getProperty());
//			allCUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CUI_CONCEPTS.getProperty());
//			allAUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.AUI_CONCEPTS.getProperty());
//
//			// Add version data to allRefsetConcept
//			eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion, BaseContentVersion.LOADER_VERSION.getProperty().getUUID(), false);
//			eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, BaseContentVersion.RELEASE.getProperty().getUUID(), false);
			
			//Disable the masterUUID debug map now that the metadata is populated, not enough memory on most systems to maintain it for everything else.
			ConverterUUID.disableUUIDMap_ = true;
			int cuiCounter = 0;

//			Statement statement = db_.getConnection().createStatement();
//			//TODO SIZELIMIT - remove SAB restriction
//			ResultSet rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
//					+ (liteLoad ? "where SAB='RXNORM' " : "") + "order by RXCUI" );
//			ArrayList<RXNCONSO> conceptData = new ArrayList<>();
//			while (rs.next())
//			{
//				RXNCONSO current = new RXNCONSO(rs);
//				if (conceptData.size() > 0 && !conceptData.get(0).rxcui.equals(current.rxcui))
//				{
//					processCUIRows(conceptData);
//					ConsoleUtil.showProgress();
//					cuiCounter++;
//					if (cuiCounter % 10000 == 0)
//					{
//						ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
//					}
//					conceptData.clear();
//				}
//				conceptData.add(current);
//			}
//			rs.close();
//			statement.close();
//
//			// process last
//			processCUIRows(conceptData);
//			
//			checkRelationships();
//
//			eConcepts_.storeRefsetConcepts(ptRefsets_, dos_);
			
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
			filesToSkip.add("MRHIER.RRF");  //don't think I need this - but maybe, to find the root concept
			filesToSkip.add("MRMAP.RRF");  //not doing mappings yet
			filesToSkip.add("MRSMAP.RRF");  //not doing mappings yet
			filesToSkip.add("CHANGE/*");  //not loading these yet
			filesToSkip.add("MRAUI.RRF");  //don't need - historical info
			filesToSkip.add("MRX*");
			
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
				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new FileReader(dataFile))), SABFilterList);
			}


			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_cui_index ON MRCONSO (CUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_cui_metaui_index ON MRSAT (CUI, METAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_metaui_index ON MRSAT (METAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_cui_index ON MRSTY (CUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON MRSTY (TUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_cui2_index ON MRREL (CUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_aui2_index ON MRREL (AUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_index ON MRREL (RELA)");
			s.close();
		}
	}

//	private void processCUIRows(ArrayList<RXNCONSO> conceptData) throws IOException, SQLException
//	{
//		EConcept cuiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("RXCUI" + conceptData.get(0).rxcui, true));
//		eConcepts_.addAdditionalIds(cuiConcept, conceptData.get(0).rxcui, ptIds_.getProperty("RXCUI").getUUID(), false);
//
//		ArrayList<ValuePropertyPairWithSAB> descriptions = new ArrayList<>();
//		
//		for (RXNCONSO rowData : conceptData)
//		{
//			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("RXAUI" + rowData.rxaui, true));
//			eConcepts_.addAdditionalIds(auiConcept, rowData.rxaui, ptIds_.getProperty("RXAUI").getUUID(), false);
//			
//			if (rowData.saui != null)
//			{
//				eConcepts_.addStringAnnotation(auiConcept, rowData.saui, ptAttributes_.getProperty("SAUI").getUUID(), false);
//			}
//			if (rowData.scui != null)
//			{
//				eConcepts_.addStringAnnotation(auiConcept, rowData.scui, ptAttributes_.getProperty("SCUI").getUUID(), false);
//			}
//			
//			eConcepts_.addUuidAnnotation(auiConcept, ptSABs_.getProperty(rowData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
//
//			if (rowData.code != null)
//			{
//				eConcepts_.addStringAnnotation(auiConcept, rowData.code, ptAttributes_.getProperty("CODE").getUUID(), false);
//			}
//
//			eConcepts_.addUuidAnnotation(auiConcept, ptSuppress_.getProperty(rowData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS")
//					.getUUID());
//			
//			if (rowData.cvf != null)
//			{
//				if (rowData.cvf.equals("4096"))
//				{
//					eConcepts_.addRefsetMember(cpcRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
//				}
//				else
//				{
//					throw new RuntimeException("Unexpected value in RXNCONSO cvf column '" + rowData.cvf + "'");
//				}
//			}
//			// TODO handle language.
//			if (!rowData.lat.equals("ENG"))
//			{
//				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
//			}
//			
//			eConcepts_.addDescription(auiConcept, rowData.str, DescriptionType.FSN, true, ptDescriptions_.getProperty(rowData.tty).getUUID(), 
//					ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
//			
//			//used for sorting description to find one for the CUI concept
//			descriptions.add(new ValuePropertyPairWithSAB(rowData.str, ptDescriptions_.getProperty(rowData.tty), rowData.sab));
//			
//			//Add attributes
//			processConceptAttributes(auiConcept, rowData.rxcui, rowData.rxaui);
//			
//			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
//			
//			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
//			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
//			addRelationships(auiConcept, rowData.rxaui, false);
//			auiConcept.writeExternal(dos_);
//		}
//		
//		//Pick the 'best' description to use on the cui concept
//		Collections.sort(descriptions);
//		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
//				ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
//		
//		//there are no attributes in rxnorm without an AUI.
//		
//		//add semantic types
//		processSemanticTypes(cuiConcept, conceptData.get(0).rxcui);
//		
//		addRelationships(cuiConcept, conceptData.get(0).rxcui, true);
//
//		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
//		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
//		cuiConcept.writeExternal(dos_);
//	}
//	
//	private void processConceptAttributes(EConcept concept, String rxcui, String rxaui) throws SQLException
//	{
//		//TODO SIZELIMIT - remove SAB restriction
//		PreparedStatement ps = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ?" 
//				+ (liteLoad ? " and (SAB='RXNORM' or ATN='NDC')" : ""));
//		ps.setString(1, rxcui);
//		ps.setString(2, rxaui);
//		ResultSet rs = ps.executeQuery();
//		
//		ArrayList<RXNSAT> rowData = new ArrayList<>();
//		while (rs.next())
//		{
//			rowData.add(new RXNSAT(rs));
//		}
//		rs.close();
//		ps.close();
//		
//		processRXNSAT(concept.getConceptAttributes(), rowData);
//	}
//	
//	private void processRXNSAT(TkComponent<?> itemToAnnotate, List<RXNSAT> rxnsatRows)
//	{
//		for (RXNSAT row : rxnsatRows)
//		{
//			//for some reason, ATUI isn't always provided - don't know why.  fallback on randomly generated in those cases.
//			TkRefsetStrMember attribute = eConcepts_.addStringAnnotation(itemToAnnotate, 
//					(row.atui == null ? null : ConverterUUID.createNamespaceUUIDFromString("ATUI" + row.atui)), row.atv, 
//					ptAttributes_.getProperty(row.atn).getUUID(), false, null);
//			
//			if (row.stype != null)
//			{
//				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(row.stype).getUUID(), ptAttributes_.getProperty("STYPE").getUUID());
//			}
//			
//			if (row.code != null)
//			{
//				eConcepts_.addStringAnnotation(attribute, row.code, ptAttributes_.getProperty("CODE").getUUID(), false);
//			}
//			if (row.atui != null)
//			{
//				eConcepts_.addStringAnnotation(attribute, row.atui, ptAttributes_.getProperty("ATUI").getUUID(), false);
//			}
//			if (row.satui != null)
//			{
//				eConcepts_.addStringAnnotation(attribute, row.satui, ptAttributes_.getProperty("SATUI").getUUID(), false);
//			}
//			eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(row.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
//			if (row.suppress != null)
//			{
//				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(row.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
//			}
//			if (row.cvf != null)
//			{
//				if (row.cvf.equals("4096"))
//				{
//					eConcepts_.addRefsetMember(cpcRefsetConcept_, attribute.getPrimordialComponentUuid(), null, true, null);
//				}
//				else
//				{
//					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + row.cvf + "'");
//				}
//			}
//		}
//	}
//	
//	private void processSemanticTypes(EConcept concept, String rxcui) throws SQLException
//	{
//		PreparedStatement ps = db_.getConnection().prepareStatement("select TUI, ATUI from RXNSTY where RXCUI = ?");
//		ps.setString(1, rxcui);
//		ResultSet rs = ps.executeQuery();
//		
//		while (rs.next())
//		{
//			if (rs.getString("ATUI") != null)
//			{
//				throw new RuntimeException("Unexpected ATUI value");
//			}
//			eConcepts_.addUuidAnnotation(concept, semanticTypes_.get(rs.getString("TUI")), ptAttributes_.getProperty("STY").getUUID());
//		}
//		rs.close();
//		ps.close();
//	}
//	
//	/**
//	 * @param isCUI - true for CUI, false for AUI
//	 * @throws SQLException
//	 */
//	private void addRelationships(EConcept concept, String id2, boolean isCUI) throws SQLException
//	{
//		//TODO SIZELIMIT - remove SAB restriction
//		Statement s = db_.getConnection().createStatement();
//		ResultSet rs = s.executeQuery("Select RXCUI1, RXAUI1, STYPE1, REL, STYPE2, RELA, RUI, SAB, RG, SUPPRESS, CVF from RXNREL where " 
//				+ (isCUI ? "RXCUI2" : "RXAUI2") + "='" + id2 + "'" + (liteLoad ? " and SAB='RXNORM'" : ""));
//				
//		while (rs.next())
//		{
//			RXNREL rel = new RXNREL(rs);
//			
//			if (isRelPrimary(rel.rel, rel.rela))
//			{
//				UUID targetConcept = ConverterUUID.createNamespaceUUIDFromString((isCUI ? "RXCUI" + rel.rxcui1 : "RXAUI" + rel.rxaui1), true);
//				TkRelationship r = eConcepts_.addRelationship(concept, (rel.rui != null ? ConverterUUID.createNamespaceUUIDFromString("RUI:" + rel.rui) : null),
//						targetConcept, ptRelationships_.getProperty(rel.rel).getUUID(), null, null, null);
//				
//				annotateRelationship(r, rel);
//				updateSanityCheckLoadedData(rel.rel, rel.rela, id2, (isCUI ? rel.rxcui1 : rel.rxaui1), (isCUI ? "CUI" : "AUI"));
//			}
//			else
//			{
//				updateSanityCheckSkipData(rel.rel, rel.rela, id2, (isCUI ? rel.rxcui1 : rel.rxaui1), (isCUI ? "CUI" : "AUI"));
//			}
//		}
//		s.close();
//		rs.close();
//	}
//	
//	private void updateSanityCheckLoadedData(String rel, String rela, String source, String target, String type)
//	{
//		loadedRels_.put(type + ":" + source, rel + ":" + rela + ":" + target);
//		skippedRels_.remove(type + ":" + source);
//	}
//	
//	private void updateSanityCheckSkipData(String rel, String rela, String source, String target, String type)
//	{
//		//Get the primary rel name, add it to the skip list
//		String primary = nameToRel_.get(rel).getFSNName();
//		String primaryExtended = null;
//		if (rela != null)
//		{
//			primaryExtended = nameToRel_.get(rela).getFSNName();
//		}
//		//also reverse the cui2 / cui1
//		skippedRels_.put(type + ":" + target, primary + ":" + primaryExtended + ":" + source);
//	}
//	
//	private void checkRelationships()
//	{
//		//if the inverse relationships all worked properly, loaded and skipped should be copies of each other.
//		
//		for (String s : loadedRels_.keySet())
//		{
//			skippedRels_.remove(s);
//		}
//		
//		if (skippedRels_.size() > 0)
//		{
//			ConsoleUtil.printErrorln("Relationship design error - " +  skippedRels_.size() + " were skipped that should have been loaded");
//			for (Entry<String, String> x : skippedRels_.entrySet())
//			{
//				ConsoleUtil.printErrorln(x.getKey() + "->" + x.getValue());
//			}
//		}
//		else
//		{
//			ConsoleUtil.println("Yea! - no missing relationships!");
//		}
//	}
//	
//	private void annotateRelationship(TkRelationship r, RXNREL relData) throws SQLException
//	{
//		eConcepts_.addStringAnnotation(r, relData.stype1, ptAttributes_.getProperty("STYPE1").getUUID(), false);
//		eConcepts_.addStringAnnotation(r, relData.stype2, ptAttributes_.getProperty("STYPE2").getUUID(), false);
//		if (relData.rela != null)
//		{
//			eConcepts_.addUuidAnnotation(r, ptRelationshipQualifiers_.getProperty(relData.rela).getUUID(), ptAttributes_.getProperty("RELA Label").getUUID());
//		}
//		if (relData.rui != null)
//		{
//			eConcepts_.addAdditionalIds(r, relData.rui, ptIds_.getProperty("RUI").getUUID());
//			Statement s = db_.getConnection().createStatement();
//			ResultSet rs = s.executeQuery("select * from RXNSAT where STYPE='RUI' and RXAUI='" + relData.rui + "'");
//			ArrayList<RXNSAT> rowData = new ArrayList<>();
//			while (rs.next())
//			{
//				rowData.add(new RXNSAT(rs));
//			}
//			rs.close();
//			s.close();
//			
//			processRXNSAT(r, rowData);
//		}
//		eConcepts_.addUuidAnnotation(r, ptSABs_.getProperty(relData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
//		if (relData.rg != null)
//		{
//			eConcepts_.addStringAnnotation(r, relData.rg, ptAttributes_.getProperty("RG").getUUID(), false);
//		}
//		if (relData.suppress != null)
//		{
//			eConcepts_.addUuidAnnotation(r, ptSuppress_.getProperty(relData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
//		}
//		if (relData.cvf != null)
//		{
//			if (relData.cvf.equals("4096"))
//			{
//				eConcepts_.addRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, true, null);
//			}
//			else
//			{
//				throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + relData.cvf + "'");
//			}
//		}
//	}
//	
	
	

//	
//	private boolean isRelPrimary(String relName, String relaName)
//	{
//		if (relaName != null)
//		{
//			return nameToRel_.get(relaName).getFSNName().equals(relaName);
//		}
//		else
//		{
//			return nameToRel_.get(relName).getFSNName().equals(relName);
//		}
//	}
//	

	
	public static void main(String[] args) throws MojoExecutionException
	{
		UMLSMojo mojo = new UMLSMojo();
		mojo.outputDirectory = new File("../UMLS-econcept/target");
		//mojo.srcDataPath = new File("/mnt/d/Work/Apelon/UMLS/extracted/2013AA/");
		mojo.srcDataPath = new File("/mnt/d/Work/Apelon/UMLS/extracted-small/2013AA/");
		//mojo.tmpDBPath = new File("/mnt/d/Scratch/");
		mojo.SABFilterList = new HashSet<String>();
		mojo.SABFilterList.add("CCS");
		mojo.execute();
	}
}
