package gov.va.umls;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion.BaseContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ValuePropertyPair;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.BaseConverter;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.propertyTypes.PT_Refsets;
import gov.va.umls.propertyTypes.PT_Attributes;
import gov.va.umls.propertyTypes.PT_IDs;
import gov.va.umls.rrf.MRCONSO;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.ihtsdo.etypes.EConcept;

public class SABLoader extends BaseConverter
{
	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	
	protected PropertyType ptSTT_Types_;
	
	public SABLoader(String sab, String terminologyName, File outputDirectory, RRFDatabaseHandle db, String loaderVersion, String releaseVersion) throws Exception
	{
		super(sab, terminologyName, db, "MR", outputDirectory, true, new PT_IDs(), new PT_Attributes());
		
		allRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.ALL.getProperty());
		allCUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CUI_CONCEPTS.getProperty());
		allAUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.AUI_CONCEPTS.getProperty());
		
		// Add version data to allRefsetConcept
		//TODO move this to a root concept, if I can find one...
		eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion, BaseContentVersion.LOADER_VERSION.getProperty().getUUID(), false);
		eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, BaseContentVersion.RELEASE.getProperty().getUUID(), false);
		
		//Disable the masterUUID debug map now that the metadata is populated, not enough memory on most systems to maintain it for everything else.
		//ConverterUUID.disableUUIDMap_ = true;
		int cuiCounter = 0;
		
		Statement statement = db_.getConnection().createStatement();
		ResultSet rs = statement.executeQuery("select * from MRCONSO where SAB='" + sab_ + "' order by CUI");
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
		
		finish();
	}
	
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
	
	private void processCUIRows(ArrayList<MRCONSO> conceptData) throws IOException, SQLException
	{
		EConcept cuiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("CUI" + conceptData.get(0).cui, true));
		eConcepts_.addAdditionalIds(cuiConcept, conceptData.get(0).cui, ptIds_.getProperty("CUI").getUUID(), false);

		ArrayList<ValuePropertyPair> descriptions = new ArrayList<>();
		
		for (MRCONSO rowData : conceptData)
		{
			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("AUI" + rowData.aui, true));
			eConcepts_.addAdditionalIds(auiConcept, rowData.aui, ptIds_.getProperty("AUI").getUUID(), false);
			
			// TODO handle language.
			if (!rowData.lat.equals("ENG"))
			{
				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
			}
			
			eConcepts_.addStringAnnotation(auiConcept, rowData.ts, ptAttributes_.getProperty("TS").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, rowData.lui, ptAttributes_.getProperty("LUI").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, rowData.stt, ptAttributes_.getProperty("STT").getUUID(), false);
			eConcepts_.addUuidAnnotation(auiConcept, ptSTT_Types_.getProperty(rowData.stt).getUUID(), ptAttributes_.getProperty("STT").getUUID());
			eConcepts_.addStringAnnotation(auiConcept, rowData.sui, ptAttributes_.getProperty("SUI").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, rowData.ispref, ptAttributes_.getProperty("ISPREF").getUUID(), false);

			
			if (rowData.saui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.saui, ptAttributes_.getProperty("SAUI").getUUID(), false);
			}
			if (rowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.scui, ptAttributes_.getProperty("SCUI").getUUID(), false);
			}
			if (rowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.sdui, ptAttributes_.getProperty("SDUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSABs_.getProperty(rowData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());

			if (rowData.code != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSourceRestrictionLevels_.getProperty(rowData.srl.toString()).getUUID(), ptAttributes_.getProperty("SRL")
					.getUUID());

			eConcepts_.addUuidAnnotation(auiConcept, ptSuppress_.getProperty(rowData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS")
					.getUUID());
			
			if (rowData.cvf != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, rowData.cvf.toString(), ptAttributes_.getProperty("CVF").getUUID(), false);
			}
			
			
			eConcepts_.addDescription(auiConcept, rowData.str, DescriptionType.FSN, true, ptDescriptions_.getProperty(rowData.tty).getUUID(), 
					ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
			
			//used for sorting description to find one for the CUI concept
			descriptions.add(new ValuePropertyPair(rowData.str, ptDescriptions_.getProperty(rowData.tty)));
			
			//Add attributes
//			processConceptAttributes(auiConcept, rowData.cui, rowData.aui);
			
			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
			
			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
//			addRelationships(auiConcept, rowData.aui, false);
			auiConcept.writeExternal(dos_);
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(descriptions);
		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
				ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
		
		//there are no attributes in rxnorm without an AUI.
		//TODO check this on umls
		
		//add semantic types
//		processSemanticTypes(cuiConcept, conceptData.get(0).cui);
		
//		addRelationships(cuiConcept, conceptData.get(0).cui, true);

		eConcepts_.addRefsetMember(allRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		eConcepts_.addRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, true, null);
		cuiConcept.writeExternal(dos_);
	}

	@Override
	protected Property makeDescriptionType(String fsnName, String preferredName, final Set<String> tty_classes)
	{
		//Will fill in the rankings below, in the allDescriptionsCreated method
		return ptDescriptions_.addProperty(fsnName, preferredName, null, false, 0);
	}


	@Override
	protected void allDescriptionsCreated() throws SQLException
	{
		/**
		 * Run through and set all of the description type ranking info.  Bump FN to the FSN category - other than that, 
		 * let the MRRANK file specify the order - putting most things in as synonyms and adding one each time we go down the rank rows.
		 */
		int fsnPos = BPT_Descriptions.FSN;
		int synonymPos = BPT_Descriptions.SYNONYM;
		
		Statement s = db_.getConnection().createStatement();
		ResultSet rs = s.executeQuery("select RANK, TTY from MRRANK where SAB='" + sab_ + "' order by RANK desc");
		int resultCount = 0;
		while (rs.next())
		{
			resultCount++;
			String tty = rs.getString("TTY");
			
			Property p = ptDescriptions_.getProperty(tty);
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
		if (ptDescriptions_.getProperties().size() > resultCount)
		{
			throw new RuntimeException("Not enough ranking data!");
		}
		rs.close();
		s.close();
	}

	@Override
	protected void addCustomRefsets() throws Exception
	{
		//noop
	}
}
