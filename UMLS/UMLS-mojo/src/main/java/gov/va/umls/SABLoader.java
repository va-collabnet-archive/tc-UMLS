package gov.va.umls;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.ihtsdo.etypes.EConcept;
import org.ihtsdo.tk.dto.concept.component.TkComponent;
import org.ihtsdo.tk.dto.concept.component.refex.type_string.TkRefsetStrMember;

public class SABLoader extends BaseConverter
{
	private EConcept allRefsetConcept_;
	private EConcept allCUIRefsetConcept_;
	private EConcept allAUIRefsetConcept_;
	
	protected PropertyType ptSTT_Types_;
	
	private PreparedStatement satAtomStatement, satConceptStatement, semanticTypeStatement, cuiRelStatementForward, auiRelStatementForward, cuiRelStatementBackward, auiRelStatementBackward;
	
	public SABLoader(String sab, String terminologyName, File outputDirectory, RRFDatabaseHandle db, String loaderVersion, String releaseVersion) throws Exception
	{
		//just putting all UMLS related terms on a 'UMLS' path, otherwise - to overwhelming to configure because of the current issues with paths in the WB.
		super(sab, terminologyName, "UMLS", db, "MR", outputDirectory, true, new PT_IDs(), new PT_Attributes());
		
		allRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.ALL.getPropertyName());
		allCUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.CUI_CONCEPTS.getPropertyName());
		allAUIRefsetConcept_ = ptRefsets_.getConcept(PT_Refsets.Refsets.AUI_CONCEPTS.getPropertyName());
		
		// Add version data to allRefsetConcept
		//TODO move this to a root concept, if I can find one...
		eConcepts_.addStringAnnotation(allRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), false);
		eConcepts_.addStringAnnotation(allRefsetConcept_, releaseVersion, ptContentVersion_.RELEASE.getUUID(), false);
		
		satAtomStatement = db_.getConnection().prepareStatement("select * from MRSAT where CUI = ? and METAUI = ? and SAB=?");
		satConceptStatement = db_.getConnection().prepareStatement("select * from MRSAT where CUI = ? and METAUI is null and SAB=?");
		semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from MRSTY where CUI = ?");
		cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT * from MRREL where CUI2 = ? and AUI2 is null and SAB='" + sab + "'");
		auiRelStatementForward = db_.getConnection().prepareStatement("SELECT * from MRREL where CUI2 = ? and AUI2 = ? and SAB='" + sab + "'");
		cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT * from MRREL where CUI1 = ? and AUI1 is null and SAB='" + sab + "'");
		auiRelStatementBackward = db_.getConnection().prepareStatement("SELECT * from MRREL where CUI1 = ? and AUI1 = ? and SAB='" + sab + "'");
		
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
		
		satAtomStatement.close();
		satConceptStatement.close();
		semanticTypeStatement.close();
		cuiRelStatementForward.close();
		cuiRelStatementBackward.close();
		auiRelStatementBackward.close();
		auiRelStatementForward.close();
		
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
		
		for (MRCONSO consoRowData : conceptData)
		{
			EConcept auiConcept = eConcepts_.createConcept(ConverterUUID.createNamespaceUUIDFromString("AUI" + consoRowData.aui, true));
			eConcepts_.addAdditionalIds(auiConcept, consoRowData.aui, ptIds_.getProperty("AUI").getUUID(), false);
			
			// TODO handle language.
			if (!consoRowData.lat.equals("ENG"))
			{
				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
			}
			
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.ts, ptAttributes_.getProperty("TS").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.lui, ptAttributes_.getProperty("LUI").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.stt, ptAttributes_.getProperty("STT").getUUID(), false);
			eConcepts_.addUuidAnnotation(auiConcept, ptSTT_Types_.getProperty(consoRowData.stt).getUUID(), ptAttributes_.getProperty("STT").getUUID());
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.sui, ptAttributes_.getProperty("SUI").getUUID(), false);
			eConcepts_.addStringAnnotation(auiConcept, consoRowData.ispref, ptAttributes_.getProperty("ISPREF").getUUID(), false);

			
			if (consoRowData.saui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.saui, ptAttributes_.getProperty("SAUI").getUUID(), false);
			}
			if (consoRowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.scui, ptAttributes_.getProperty("SCUI").getUUID(), false);
			}
			if (consoRowData.scui != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.sdui, ptAttributes_.getProperty("SDUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSABs_.getProperty(consoRowData.sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());

			if (consoRowData.code != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(auiConcept, ptSourceRestrictionLevels_.getProperty(consoRowData.srl.toString()).getUUID(), ptAttributes_.getProperty("SRL")
					.getUUID());

			eConcepts_.addUuidAnnotation(auiConcept, ptSuppress_.getProperty(consoRowData.suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS")
					.getUUID());
			
			if (consoRowData.cvf != null)
			{
				eConcepts_.addStringAnnotation(auiConcept, consoRowData.cvf.toString(), ptAttributes_.getProperty("CVF").getUUID(), false);
			}
			
			
			eConcepts_.addDescription(auiConcept, consoRowData.str, DescriptionType.FSN, true, ptDescriptions_.getProperty(consoRowData.tty).getUUID(), 
					ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
			
			//used for sorting description to find one for the CUI concept
			descriptions.add(new ValuePropertyPair(consoRowData.str, ptDescriptions_.getProperty(consoRowData.tty)));
			
			//Add Atom attributes
			satAtomStatement.clearParameters();
			satAtomStatement.setString(1, consoRowData.cui);
			satAtomStatement.setString(2, consoRowData.aui);
			satAtomStatement.setString(3, sab_);
			ResultSet rs = satAtomStatement.executeQuery();
			processSAT(auiConcept.getConceptAttributes(), rs);
			
			//Add rel to parent CUI
			eConcepts_.addRelationship(auiConcept, cuiConcept.getPrimordialUuid());
			
			eConcepts_.addRefsetMember(allRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			eConcepts_.addRefsetMember(allAUIRefsetConcept_, auiConcept.getPrimordialUuid(), null, true, null);
			
			auiRelStatementForward.clearParameters();
			auiRelStatementForward.setString(1, conceptData.get(0).cui);
			auiRelStatementForward.setString(2, consoRowData.aui);
			addRelationships(auiConcept, auiRelStatementForward.executeQuery(), true);
			
			auiRelStatementBackward.clearParameters();
			auiRelStatementBackward.setString(1, conceptData.get(0).cui);
			auiRelStatementBackward.setString(2, consoRowData.aui);
			addRelationships(auiConcept, auiRelStatementBackward.executeQuery(), false);
			auiConcept.writeExternal(dos_);
		}
		
		//Pick the 'best' description to use on the cui concept
		Collections.sort(descriptions);
		eConcepts_.addDescription(cuiConcept, descriptions.get(0).getValue(), DescriptionType.FSN, true, descriptions.get(0).getProperty().getUUID(), 
				ptDescriptions_.getPropertyTypeReferenceSetUUID(), false);
		
		//process concept attributes
		satConceptStatement.clearParameters();
		satConceptStatement.setString(1, conceptData.get(0).cui);
		satConceptStatement.setString(2, sab_);
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
					ptAttributes_.getProperty(atn).getUUID(), false, null);
			
			if (lui != null)
			{
				eConcepts_.addStringAnnotation(attribute, lui, ptAttributes_.getProperty("LUI").getUUID(), false);
			}
			
			if (sui != null)
			{
				eConcepts_.addStringAnnotation(attribute, sui, ptAttributes_.getProperty("SUI").getUUID(), false);
			}
			
			if (stype != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptAttributes_.getProperty("STYPE").getUUID());
			}
			
			if (code != null)
			{
				eConcepts_.addStringAnnotation(attribute, code, ptAttributes_.getProperty("CODE").getUUID(), false);
			}
			
			if (atui != null)
			{
				eConcepts_.addStringAnnotation(attribute, atui, ptAttributes_.getProperty("ATUI").getUUID(), false);
			}
			
			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptAttributes_.getProperty("SATUI").getUUID(), false);
			}
			
			eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptAttributes_.getProperty("SAB").getUUID());
			
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				eConcepts_.addStringAnnotation(attribute, cvf.toString(), ptAttributes_.getProperty("CVF").getUUID(), false);
			}
		}
		rs.close();
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
