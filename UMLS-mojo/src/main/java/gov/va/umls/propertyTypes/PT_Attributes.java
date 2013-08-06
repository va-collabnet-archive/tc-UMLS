package gov.va.umls.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Attributes;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Attributes extends BPT_Attributes
{
	public PT_Attributes()
	{
		addProperty("AUI", null, "Unique identifier for atom - variable length field, 8 or 9 characters");  //Loaded an an attribute and an ID
		addProperty("tty_class");
		addProperty("STN", "Semantic Type tree number", null);
		addProperty("STY", "Semantic Type", null);
		addProperty("URI");
		addProperty("TS", null, "Term Status");
		addProperty("LUI", null, "Unique identifier for term");
		addProperty("SUI", null, "Unique identifier for string");
		addProperty("STT", null, "String Type");
		addProperty("ISPREF", null, "Atom status - preferred (Y) or not (N) for this string within this concept");
		addProperty("SAUI", null, "Source asserted atom identifier [optional]");
		addProperty("SCUI", null, "Source asserted concept identifier [optional]");
		addProperty("SDUI", null, "Source asserted descriptor identifier [optional]");
		addProperty("SAB", null, "Abbreviated source name (SAB)");
		addProperty("CODE", null, "Most useful source asserted identifier (if the source vocabulary has more than one identifier), or a Metathesaurus-generated source entry identifier (if the source vocabulary has none)");
		addProperty("SRL", null, "Source restriction level");
		addProperty("SUPPRESS", null, "Suppressible flag. Values = O, E, Y, or N");
		addProperty("CVF", null, "Content View Flag. Bit field used to flag rows included in Content View.");
		addProperty("METAUI", null, "Metathesaurus atom identifier (will have a leading A) or Metathesaurus relationship identifier (will have a leading R) or blank if it is a concept attribute.");
		addProperty("STYPE", null, "The name of the column in MRCONSO.RRF or MRREL.RRF that contains the identifier to which the attribute is attached, i.e. AUI, CODE, CUI, RUI, SCUI, SDUI.");
		addProperty("SATUI", null, "Source asserted attribute identifier (optional - present if it exists)");
		addProperty("STYPE1", null, "The name of the column in MRCONSO.RRF that contains the identifier used for the first element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.");
		addProperty("STYPE2", null, "The name of the column in MRCONSO.RRF that contains the identifier used for the second element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.");
		addProperty("Generic rel type", null, "Generic rel type for this relationship");
		addProperty("Generic rel type (inverse)", null, "Generic rel type for this relationship - however - the inverse of the generic rel type was indicated (mismatch between RELA primary/inverse naming and generic primary/inverse naming)");
		addProperty("SRUI", null, "Source asserted relationship identifier, if present");
		addProperty("SL", null, "Source of relationship labels");
		addProperty("RG", null, "Relationship group. Used to indicate that a set of relationships should be looked at in conjunction.");
		addProperty("DIR", null, "Source asserted directionality flag. Y indicates that this is the direction of the relationship in its source; N indicates that it is not; a blank indicates that it is not important or has not yet been determined.");
	}
}
