package gov.va.umls.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Annotations;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Annotations extends BPT_Annotations
{
	public PT_Annotations()
	{
		indexByAltNames();
		addProperty("Unique identifier for atom", null, "AUI", "variable length field, 8 or 9 characters");  //Loaded an an attribute and an ID
		addProperty("Term Type Class", null, "tty_class", null);
		addProperty("Semantic Type tree number", null, "STN", null);
		addProperty("Semantic Type", null, "STY", null);
		addProperty("URI");
		addProperty("Term Status", null, "TS", null);
		addProperty("Unique identifier for term", null, "LUI", null);
		addProperty("Unique identifier for string", null, "SUI", null);
		addProperty("String Type", null, "STT", null);
		addProperty("Is Preferred", null, "ISPREF", "Atom status - preferred (Y) or not (N) for this string within this concept");
		addProperty("Source asserted atom identifier", null, "SAUI", "[optional]");
		addProperty("Source asserted concept identifier", null, "SCUI", " [optional]");
		addProperty("Source asserted descriptor identifier", null, "SDUI", "[optional]");
		addProperty("Abbreviated source name", null, "SAB", null);
		addProperty("Code", null, "CODE", "Most useful source asserted identifier (if the source vocabulary has more than one identifier), or a Metathesaurus-generated source entry identifier (if the source vocabulary has none)");
		addProperty("Source restriction level", null, "SRL", null);
		addProperty("Suppress", null, "SUPPRESS", "Suppressible flag. Values = O, E, Y, or N");
		addProperty("Content View Flag", null, "CVF", "Bit field used to flag rows included in Content View.");
		addProperty("Metathesaurus identifier", null, "METAUI", "Metathesaurus atom identifier (will have a leading A) or Metathesaurus relationship identifier (will have a leading R) or blank if it is a concept attribute.");
		addProperty("STYPE", null, "The name of the column in MRCONSO.RRF or MRREL.RRF that contains the identifier to which the attribute is attached, i.e. AUI, CODE, CUI, RUI, SCUI, SDUI.");
		addProperty("Source asserted attribute identifier", null, "SATUI", "(optional - present if it exists)");
		addProperty("STYPE1", null, "The name of the column in MRCONSO.RRF that contains the identifier used for the first element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.");
		addProperty("STYPE2", null, "The name of the column in MRCONSO.RRF that contains the identifier used for the second element in the relationship, i.e. AUI, CODE, CUI, SCUI, SDUI.");
		addProperty("Generic rel type", null, "Generic rel type for this relationship");
		addProperty("Generic rel type (inverse)", null, "Generic rel type for this relationship - however - the inverse of the generic rel type was indicated (mismatch between RELA primary/inverse naming and generic primary/inverse naming)");
		addProperty("Source asserted relationship identifier", null, "SRUI", "Source asserted relationship identifier, if present");
		addProperty("Source of relationship labels", null, "SL", null);
		addProperty("Relationship group", null, "RG", "Used to indicate that a set of relationships should be looked at in conjunction.");
		addProperty("Source asserted directionality flag", null, "DIR", "Source asserted directionality flag. Y indicates that this is the direction of the relationship in its source; N indicates that it is not; a blank indicates that it is not important or has not yet been determined.");
	}
}
