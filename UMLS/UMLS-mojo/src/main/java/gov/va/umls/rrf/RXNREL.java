package gov.va.umls.rrf;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RXNREL
{
	public String rxcui1, rxaui1, stype1, rel, stype2, rela, rui, sab, rg, suppress, cvf;

	public RXNREL(ResultSet rs) throws SQLException
	{
		rxcui1 = rs.getString("RXCUI1");
		rxaui1 = rs.getString("RXAUI1");
		stype1 = rs.getString("STYPE1");
		rel = rs.getString("REL");
		stype2 = rs.getString("STYPE2");
		rela = rs.getString("RELA");
		rui = rs.getString("RUI");
		sab = rs.getString("SAB");
		rg = rs.getString("RG");
		suppress = rs.getString("SUPPRESS");
		cvf = rs.getString("CVF");
	}
}
