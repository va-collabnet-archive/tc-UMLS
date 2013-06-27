package gov.va.umls;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal to convert UMLS data
 * 
 * @goal buildUMLS
 * 
 * @phase process-sources
 */
public class UMLSMojo extends AbstractMojo
{
	private RRFDatabaseHandle db_;

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
			
			if (sabFilters == null || sabFilters.size() == 0)
			{
				sabFilters = new ArrayList<>();
				Statement s = db_.getConnection().createStatement();
				ResultSet rs = s.executeQuery("Select distinct SAB from MRRANK");
				while (rs.next())
				{
					String sab = rs.getString("SAB");
					sabFilters.add(sab);
				}
				rs.close();
				s.close();
			}
			
			for (String sab : sabFilters)
			{
				Statement s = db_.getConnection().createStatement();
				ResultSet rs = s.executeQuery("Select SSN from MRSAB where RSAB ='" + sab + "' or VSAB='" + sab + "'");
				String terminologyName = "";
				if (rs.next())
				{
					terminologyName = rs.getString("SSN");
				}
				else
				{
					throw new RuntimeException("Can't find name for " + sab);
				}
				new SABLoader(sab, terminologyName, outputDirectory, db_, loaderVersion, releaseVersion);
			}
			
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
				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new FileReader(dataFile))), sabFilters);
			}

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_cui_index ON MRCONSO (CUI)");  //make order by fast
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
			s.close();
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
