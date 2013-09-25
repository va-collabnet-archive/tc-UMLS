package gov.va.umls.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_IDs;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_IDs extends BPT_IDs
{
	public PT_IDs()
	{
		super();
		indexByAltNames();
		addProperty("Unique identifier for concept", null, "CUI", null);
		addProperty("Unique identifier for atom", null, "AUI", "variable length field, 8 or 9 characters");
		addProperty("Unique identifier of Semantic Type", null, "TUI", null);
		addProperty("Unique identifier of Relationship", null, "RUI", null);
		addProperty("Unique identifier of Attribute", null, "ATUI", null);
	}
}
