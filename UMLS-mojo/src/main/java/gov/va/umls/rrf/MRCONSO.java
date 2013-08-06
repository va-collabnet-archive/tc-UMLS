package gov.va.umls.rrf;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MRCONSO
{
	public String cui, lat, ts, lui, stt, sui, ispref, aui, saui, scui, sdui, sab, tty, code, str, suppress;
	public Integer srl, cvf;

	public MRCONSO(ResultSet rs) throws SQLException
	{
		cui = rs.getString("CUI");
		lat = rs.getString("LAT");
		ts = rs.getString("TS");
		lui = rs.getString("LUI");
		stt = rs.getString("STT");
		sui = rs.getString("SUI");
		ispref = rs.getString("ISPREF");
		aui = rs.getString("AUI");
		saui = rs.getString("SAUI");
		scui = rs.getString("SCUI");
		sdui = rs.getString("SDUI");
		sab = rs.getString("SAB");
		tty = rs.getString("TTY");
		code = rs.getString("CODE");
		str = rs.getString("STR");
		srl = rs.getObject("SRL") == null ? null : rs.getInt("SRL");
		suppress = rs.getString("SUPPRESS");
		cvf = rs.getObject("CVF") == null ? null : rs.getInt("CVF");
	}
}
