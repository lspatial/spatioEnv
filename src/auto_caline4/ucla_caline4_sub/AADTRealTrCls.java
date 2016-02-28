package ucla_caline4_sub;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.postgresql.jdbc2.TimestampUtils;

public class AADTRealTrCls extends PgsqlDbNode {

	static ArrayList m_meteoArr = new ArrayList();
	int m_bufdis=500; 
	private static final Timestamp Calendar = null;

	String m_subtb;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

	public AADTRealTrCls() {
	};

	public void inputTrData(String fpath) throws SQLException {
		Vector v = new Vector();
		File f = new File(fpath);// 文件夹路径
		if (f.isDirectory()) {
			String[] str = null;
			str = f.list();
			for (int i = 0; i < str.length; i++) {
				String s = str[i];
				if (s.indexOf("Nd12_text_station_hour_") != -1)
					v.add(str[i]);
			}
		}
		for (int i = 0; i < v.size(); i++) {
			String atbname = (String) v.get(i);
			System.out.println("staingting the input for " + atbname);
			int nm_st = 0;
			int nm_end = atbname.lastIndexOf(".txt");
			String tname = atbname.substring(nm_st, nm_end);
			System.out.println(tname + "\n");
			String sqlStr = "DROP TABLE IF EXISTS " + tname + " cascade";
			System.out.println(sqlStr + "\n");
			Statement st = m_pcon.createStatement();
			st.execute(sqlStr);
			sqlStr = "CREATE TABLE "
					+ tname
					+ "  \n"
					+ "( tstamp timestamp without time zone, stationid character varying, rout integer, direction character varying(5),\n"
					+ "  stationtype character varying, stationlength double precision, samples integer, observed double precision, \n"
					+ "  totalflow integer, avgoccu double precision) WITH (OIDS=TRUE) ";
			st.execute("SET datestyle = 'ISO, MDY'");
			st.execute(sqlStr);
			sqlStr = "copy " + tname + "  from '" + fpath + atbname + "' CSV";
			st.execute(sqlStr);
			sqlStr = "CREATE INDEX " + tname + "_idx ON " + tname
					+ " USING btree(tstamp)";
			st.execute(sqlStr);
			st.close();
		}
	}

	public ArrayList getMeteoInfo() {
		return m_meteoArr;
	}
	
	
	public static void readASubMeteoInfor(int model_id, String subs_tb, String mtb)
			throws SQLException {
		m_meteoArr.clear();
		m_meteoArr=null;
		m_meteoArr = new ArrayList(); 
		String everything = "";
		Statement st = m_pcon.createStatement();
		String sqlStr = "select a.*, cast(a.tmpstamp as date) as tdate, extract(hour from a.tmpstamp) as hr from " + mtb
				+ " a inner join (select model_id, com, edc_date, dob_date from "+subs_tb+" where model_id="+model_id+")s "+
				  "   on a.com=s.com and a.tmpstamp between s.edc_date and (s.dob_date + interval '23 hours' )order by tmpstamp ";
		ResultSet rset = st.executeQuery(sqlStr);
		String day_type = "day";
		double wn_dir = 0, wn_spd = 0, mixh = 500, sigth = 0, amb = 0, temp = 0;
		int stab = 1;
		int i = 1;
		String tsampStr = "";
		Timestamp tstamp = null;
		DecimalFormat myfmt = new DecimalFormat("#0.0");
		boolean tmstart=true;
		while (rset.next()) {
			// day_type = rset.getString("type");
			wn_dir = rset.getInt("wd");
			wn_spd = rset.getDouble("ws");
			mixh = rset.getDouble("mh");
			sigth = rset.getInt("sigth");
			stab = (int) rset.getInt("stability");
			temp = rset.getDouble("temp");
			tstamp = rset.getTimestamp("tmpstamp");
			//tsampStr = tstamp.toString();
			int hr = rset.getInt("hr");
			if (hr < 10) {
				tsampStr = rset.getDate("tdate").toString() + " 0"
						+ String.valueOf(hr);
			} else {
				tsampStr = rset.getDate("tdate").toString() + " "
						+ String.valueOf(hr);
			}
			String[] lines = new String[2];
			if(tmstart){
				lines[0] = "11101     " + String.valueOf(i) + "   " + tsampStr ; 
						//+ "  " + day_type;
                tmstart=false;				
			}else{
				lines[0] = "10001     " + String.valueOf(i) + "   " + tsampStr ;
			}
     	    lines[1] = String.valueOf(myfmt.format(wn_dir)) + "  "
					+ String.valueOf(myfmt.format(wn_spd)) + " "
					+ String.valueOf(stab) + "   "
					+ String.valueOf(myfmt.format(mixh)) + "  "
					+ String.valueOf(myfmt.format(sigth)) + "  0.0  "
					+ String.valueOf(myfmt.format(temp)) + " ";
			m_meteoArr.add(lines);
			i++;
		}
		rset.close();
		st.close();
	}

	
	private ResultSet setCurPos(ResultSet rSet, int pos) throws SQLException {
		rSet.first();
		for (int i = 1; i < pos; i++)
			rSet.next();
		return rSet;
	}

	public void extractSingleAADT(String inTb, String aadt_tb, String outpath, int bufdis, String pol, int _emission) throws SQLException {
		m_bufdis=bufdis;
		m_subtb=inTb; 
		PreparedStatement yps = m_pcon
				.prepareStatement(
						"select  min(ST_X(geom)) as minx,max(ST_X(geom)) as maxx,min(ST_Y(geom)) as miny,max(ST_Y(geom)) as maxy from "
								+ inTb, ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
		ResultSet ymap = yps.executeQuery();
		ymap.next();
		double xmin = ymap.getDouble("minx");
		double xmax = ymap.getDouble("maxx");
		double ymin = ymap.getDouble("miny");
		double ymax = ymap.getDouble("maxy");
		double extx = 10000, exty = 10000;
		m_basicpath = outpath;
		String expandMtxt = "LINESTRING(" + Double.toString(xmin - extx) + " "
				+ Double.toString(ymin - exty) + ","
				+ Double.toString(xmax + extx) + " "
				+ Double.toString(ymax + exty) + ")";
		Statement st = m_pcon.createStatement();
		m_pcon.setAutoCommit(true);
		st.execute("DROP TABLE IF EXISTS aadt02_m_tmp cascade ");
		String sqlStr = "create table aadt02_m_tmp as \n"
				+ "  select * from "+aadt_tb+" where geom && ST_Expand(ST_GeomFromText('"
				+ expandMtxt + "'),100)";
		st.execute(sqlStr);
		st.execute("DROP TABLE IF EXISTS geo_aadt_tmp cascade");
		sqlStr = "create table geo_aadt_tmp as \n"
				+ "  select distinct p.gid as pgid,p.adjaadt, a.gid as aadt_gid,a.aadt,hdv,ldv, a.fhdv, a.street_nam as street_name,a.number_of_ as lanes,a.funccl,\n"
				+ "       p.geom as org_geom, (ST_Dump(ST_Intersection(a.geom, p.bgeom))).geom"
				+ "    from (select model_id as gid, adjaadt, geom,(ST_Dump(ST_Buffer(geom,"
				+ String.valueOf(bufdis) + "))).geom as bgeom from " + inTb
				+ " ) p inner join aadt02_m_tmp a \n"
				+ "       on   ST_Intersects(a.geom,p.bgeom) \n"
				+ "    order by pgid ";
		st.execute(sqlStr);
		st.execute("DROP TABLE IF EXISTS my_inx cascade");
		st.execute("CREATE INDEX my_inx ON geo_aadt_tmp (pgid)");
		st.execute("DROP View IF EXISTS geo_aadt_tmp_vn cascade");
		sqlStr = "create or replace view geo_aadt_tmp_vn as select * from geo_aadt_tmp";
		st.execute(sqlStr);
		extractAADTRoadInfo(st, "geo_aadt_tmp_vn", "pgid","",pol,_emission);
		yps.close();
		yps=null; 
		st.close(); 
		st=null; 
	}

	public void extractAADTRoadInfo(Statement st, String tbname, String idStr, String sub_series_tb, String pol, int _emission)
			throws SQLException {
		String sqlStr = "";
		PreparedStatement ashps_pst = m_pcon.prepareStatement("select " + idStr
				+ " from " + tbname + " group by " + idStr + "");
		ResultSet ashps = ashps_pst.executeQuery();
		while (ashps.next()) {
			long ash_stTime = System.currentTimeMillis();
			int pgid = ashps.getInt(idStr);
			// st = m_pcon.createStatement();
			st.execute("drop view if exists ashp_lines cascade");
			sqlStr = "create or replace view ashp_lines as "
					+ "  select row_number() over () as icnt,adjaadt,aadt_gid,street_name,lanes,aadt,hdv,ldv,funccl, fhdv, geom,ST_X(org_geom) as loc_x, ST_Y(org_geom) as loc_y "
					+ "    from " + tbname + " where pgid="
					+ Integer.toString(pgid);
			st.execute(sqlStr);
			PreparedStatement selv_pst = m_pcon.prepareStatement(
					"select * from ashp_lines",
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			ResultSet selv = selv_pst.executeQuery();
			int igm = 1;
			if (!selv.next())
				break;
			double hloc_x = selv.getDouble("loc_x");
			double hloc_y = selv.getDouble("loc_y");
			double adjaadt = selv.getDouble("adjaadt");
			String aline_out = Integer.toString(pgid);
			ArrayList<RoadInfo> myroadinf = new ArrayList<RoadInfo>();
			do{
				st.execute("drop view if exists alinepoints_tb cascade");
				sqlStr = "create or replace view alinepoints_tb as "
						+ "  select* from ashp_lines where icnt="
						+ Integer.toString(igm);
				st.execute(sqlStr);
				sqlStr = "select *, ST_NPoints(geom) as pntsum,ST_X(ST_PointN(geom,1)) as x1st, ST_Y(ST_PointN(geom,1)) as y1st,"
						+ "ST_X(ST_PointN(geom,ST_NPoints(geom))) as xend, ST_Y(ST_PointN(geom,ST_NPoints(geom)))  as yend,"
						+ "ST_Length(geom) as lng  from alinepoints_tb";
				PreparedStatement PointSet_pst = m_pcon
						.prepareStatement(sqlStr);
				ResultSet PointSet = PointSet_pst.executeQuery();
				if (!PointSet.next()) {
					break;
				}
				int aadt_gid = PointSet.getInt("aadt_gid");
				double ppx = PointSet.getDouble("x1st");
				double ppy = PointSet.getDouble("y1st");
				double endx = PointSet.getDouble("xend");
				double endy = PointSet.getDouble("yend");
				double length = PointSet.getDouble("lng");
				int PointN2 = PointSet.getInt("pntsum");
				String street_name_v = PointSet.getString("street_name");
				int lanes_v = PointSet.getInt("lanes");
				int funccl = PointSet.getInt("funccl");
				int aadt_v = PointSet.getInt("aadt");
				int hdv_v = PointSet.getInt("hdv");
				int ldv_v = PointSet.getInt("ldv");
				double fhdv=PointSet.getDouble("fhdv"); 
				if (PointN2 == 2 && length >= 20) {
					length = Math.sqrt(Math.pow(endx - ppx, 2.0)
							+ Math.pow(endy - ppy, 2.0));
					if (length > 21) {
						RoadInfo myrd_inf = new RoadInfo();
						myrd_inf.aadt_gid = aadt_gid;
						myrd_inf.street_name = street_name_v;
						myrd_inf.lanes = lanes_v;
						myrd_inf.funccl = funccl;
						myrd_inf.aadt =(int)(adjaadt*aadt_v/24.0);
						myrd_inf.hdv = (int)(adjaadt*hdv_v);
						myrd_inf.ldv = (int)(adjaadt*ldv_v);
						myrd_inf.start_x = ppx;
						myrd_inf.start_y = ppy;
						myrd_inf.end_x = endx;
						myrd_inf.end_y = endy;
						myrd_inf.length = length;
						myrd_inf.fhdv=fhdv; 
						myroadinf.add(myrd_inf);
					}
				} else if (PointN2 > 2 && length >= 20) {
					RoadInfo myrd_inf = new RoadInfo();
					myrd_inf.aadt_gid = aadt_gid;
					myrd_inf.street_name = street_name_v;
					myrd_inf.lanes = lanes_v;
					myrd_inf.funccl = funccl;
					myrd_inf.aadt =(int)(adjaadt*aadt_v/24.0);
					myrd_inf.hdv = (int)(adjaadt*hdv_v);
					myrd_inf.ldv = (int)(adjaadt*ldv_v);
					myrd_inf.start_x = ppx;
					myrd_inf.start_y = ppy;
					myrd_inf.fhdv=fhdv;  
					myroadinf.add(myrd_inf);
					int ilp = 1;
					double accang = 0.0;
					double tllength = 0.0;
					while (ilp < PointN2 - 1) {
						int ilp1 = ilp + 1;
						int ilp2 = ilp + 2;
						sqlStr = "select degrees(ST_Azimuth(gm1,gm2)-ST_Azimuth(gm,gm1)) as deg, ST_X(gm) as stx, ST_Y(gm)  as sty,"
								+ "         ST_X(gm1) as st1x, ST_Y(gm1) as st1y "
								+ "  from (select ST_PointN(geom,?) as gm, ST_PointN(geom,?) as gm1,ST_PointN(geom,?) as gm2 from alinepoints_tb)a";
						PreparedStatement agl_pst = m_pcon
								.prepareStatement(sqlStr);
						agl_pst.setInt(1, ilp);
						agl_pst.setInt(2, ilp1);
						agl_pst.setInt(3, ilp2);
						ResultSet agl = agl_pst.executeQuery();
						if (agl.next()) {
							double agl2 = agl.getDouble("deg");
							accang = accang + agl2;
							double accang2 = accang % 360.0;
							if (((agl2 > 20 && agl2 < 340) || (agl2 < -20 && agl2 > -340))
									|| ((accang2 > 20 && accang2 < 340) || (accang2 < -20 && accang2 > -340))) {
								double aendx = agl.getDouble("st1x");
								double aendy = agl.getDouble("st1y");
								tllength = Math.sqrt(Math.pow(aendx - ppx, 2.0)
										+ Math.pow(aendy - ppy, 2.0));
								if (tllength >= 21) {
									myrd_inf = (RoadInfo) myroadinf
											.get(myroadinf.size() - 1);
									myrd_inf.end_x = aendx;
									myrd_inf.end_y = aendy;
									myrd_inf.length = tllength;
									// add a new segment
									myrd_inf = new RoadInfo();
									myrd_inf.aadt_gid = aadt_gid;
									myrd_inf.street_name = street_name_v;
									myrd_inf.lanes = lanes_v;
									myrd_inf.funccl = funccl;
									myrd_inf.aadt =(int)(adjaadt*aadt_v/24.0);
									myrd_inf.hdv = (int)(adjaadt*hdv_v);
									myrd_inf.ldv = (int)(adjaadt*ldv_v);
									myrd_inf.start_x = aendx;
									myrd_inf.start_y = aendy;
									myrd_inf.fhdv=fhdv; 
									myroadinf.add(myrd_inf);
								} else {
									myrd_inf = (RoadInfo) myroadinf
											.get(myroadinf.size() - 1);
									myrd_inf.start_x = aendx;
									myrd_inf.start_y = aendy;
								}
								ppx = aendx;
								ppy = aendy;
								accang = 0.0;
							}
						}
						ilp = ilp + 1;
						agl.close();
						agl_pst.close();
					}
					tllength = Math.sqrt(Math.pow(endx - ppx, 2.0)
							+ Math.pow(endy - ppy, 2.0));
					if (tllength >= 21) {
						myrd_inf = (RoadInfo) myroadinf
								.get(myroadinf.size() - 1);
						myrd_inf.end_x = endx;
						myrd_inf.end_y = endy;
						myrd_inf.length = tllength;
					} else {
						int rpos = myroadinf.size() - 1;
						if (rpos >= 0)
							myroadinf.remove(myroadinf.size() - 1);
					}
				}
				igm = igm + 1;
				PointSet.close();
				PointSet_pst.close();
			}while(selv.next());
	
			String majorname = aline_out;
			long ash_endTime = System.currentTimeMillis();
			long ash_spTime = ash_endTime - ash_stTime;

			OutputFile_Thd myoutthd = new OutputFile_Thd(aline_out
					+ "txtout_thread", -1, ash_spTime, 1,pol,m_bufdis);
			myoutthd.SetDataFormat(pgid, myroadinf, hloc_x, hloc_y, _emission);  
			readASubMeteoInfor(pgid,m_subtb,"uclacl4_met_final");
			myoutthd.SetOutput(m_basicpath, majorname, m_meteoArr, -1, -1);
			myoutthd.start();
			selv.close();
			selv_pst.close();
		}
	}
}
