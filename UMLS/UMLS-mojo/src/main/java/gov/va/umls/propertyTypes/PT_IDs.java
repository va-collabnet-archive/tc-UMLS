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
//		addProperty("RXCUI", null, "RxNorm Unique identifier for concept (concept ID)");
//		addProperty("RXAUI", null, "Unique identifier for atom (RxNorm Atom Id)");  //loaded as an attribute and a id
		addProperty("TUI", null, "Unique identifier of Semantic Type");
//		addProperty("RUI", null, "Unique identifier for Relationship");
	}
}
