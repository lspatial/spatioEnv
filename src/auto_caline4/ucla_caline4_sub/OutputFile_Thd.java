package ucla_caline4_sub;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList; 
import java.util.HashMap;

public class OutputFile_Thd extends Thread {

	   int m_threadNo=0;
	   long m_altime=0;
	   String m_basicpath,m_majorname;
	   String m_line4 ;
	   String m_first_inf;
	   String m_flow_inf;
	   String m_aadt_inf;
	   String m_hdv_inf;
	   String m_ldv_inf;
	   String m_trftype="AADT";
	   String m_combinedlines="";
	   int m_bufdist=0;
	   String m_pol="1CO"; 
	   float m_emission=10;
	   int m_rdsum; 
	   ArrayList m_meteoArr=null;
	   int pgid;
	   ArrayList<RoadInfo> myroadinf;
	   double  hloc_x, hloc_y;
	   int m_iXInt,m_iYInt; 
	   int m_realouttype=0; 
	   
	   public OutputFile_Thd(String threadName, int cuThreadNo,long alr_time,int realouttype,String _pol, int _bufdist) {
	        super(threadName);
	        m_threadNo=cuThreadNo;
	        m_altime=alr_time;
	        m_realouttype=realouttype;
	        m_bufdist=_bufdist;
	        m_pol=_pol;
	   }
	   
	   public void setTrfType(String _type){
		   m_trftype=_type;
	   }
	   
   
	   public void SetOutput(String basicpath,String majorname, ArrayList meteoArr,int iXInt,int iYInt){
		   m_basicpath=basicpath;
		   m_majorname=majorname;
		   m_meteoArr=meteoArr;
		   m_iXInt=iXInt;
		   m_iYInt=iYInt;
	   }
	 
	   public void run() {
		   if(m_trftype.equals("AADT")){
			   getAADTOut();
		   }else if(m_trftype.equals("realtime")){
			   getRelatimeTrfOut();
		   }
	   }
	   
	   public void getRelatimeTrfOut() {
		   String myname=getName();
		   System.out.println(myname+ " txt output thread starting ... !");
		   long startTime = System.currentTimeMillis();
		   if(m_realouttype==0){
			   RealTrfDataFormat();
		   }else if(m_realouttype==1){
			   RealTrfDataFormat_avg();
		   }else{} 
		   String hdv_dfile="", ldv_dfile="",afile="";
		   if(m_iXInt==-1){
			   afile=m_basicpath+"/realtime/"+m_majorname+".txt";
			   //hdv_dfile=m_basicpath+"/hdv_out/"+m_majorname+"_10.txt";
			   //ldv_dfile=m_basicpath+"/ldv_out/"+m_majorname+".txt";
		   }else{
			   hdv_dfile=m_basicpath+"/hdv_out/x_"+Integer.toString(m_iXInt)+"/y_"+Integer.toString(m_iYInt)+"/"+m_majorname+"_10.txt";
			   ldv_dfile=m_basicpath+"/ldv_out/x_"+Integer.toString(m_iXInt)+"/y_"+Integer.toString(m_iYInt)+"/"+m_majorname+".txt";
		   }
		   //String sppath=m_basicpath+"/hdv_out/"+m_majorname+"_10.txt";
		   realTrf_outDisf(afile,0);
		   //sppath=m_basicpath+"/ldv_out/"+m_majorname+".txt";
		   //outDisf(ldv_dfile,2);
		   long endTime = System.currentTimeMillis();
		   myroadinf.clear();
		   myroadinf=null;
		   m_first_inf=null;
		   m_flow_inf=null; 
		   m_hdv_inf=null;
		   m_ldv_inf=null;
		   System.out.println("get traffic info：" + (m_altime) + "ms");
		   System.out.println("write txt ：" + (endTime - startTime) + "ms"); 
	  }
	   
	   
	   public void getAADTOut() {
		   String myname=getName();
		   System.out.println(myname+ " txt output thread starting ... !");
		   long startTime = System.currentTimeMillis();
		   DataFormat();
		   String hdv_dfile="", ldv_dfile="",aadt_dfile="";
		   if(m_iXInt==-1){
			   hdv_dfile=m_basicpath+"/hdv/"+m_majorname+".txt";
			   ldv_dfile=m_basicpath+"/ldv/"+m_majorname+".txt";
			   aadt_dfile=m_basicpath+"/aadt/"+m_majorname+".txt";
		   }else{
			   hdv_dfile=m_basicpath+"/hdv/x_"+Integer.toString(m_iXInt)+"/y_"+Integer.toString(m_iYInt)+"/"+m_majorname+"_10.txt";
			   ldv_dfile=m_basicpath+"/ldv/x_"+Integer.toString(m_iXInt)+"/y_"+Integer.toString(m_iYInt)+"/"+m_majorname+".txt";
			   aadt_dfile=m_basicpath+"/aadt/x_"+Integer.toString(m_iXInt)+"/y_"+Integer.toString(m_iYInt)+"/"+m_majorname+".txt";
		   }
		   //String sppath=m_basicpath+"/hdv_out/"+m_majorname+"_10.txt";
		   if(m_pol.equals("6UFP")){
			   outDisf(aadt_dfile,0,m_pol,m_bufdist);
		   }else if(m_pol.equals("1CO")){
			   outDisf(hdv_dfile,1,m_pol,m_bufdist);
			   outDisf(ldv_dfile,2,m_pol,m_bufdist);
	       }
		   //outDisf(hdv_dfile,1,m_pol,m_bufdist);
		   //sppath=m_basicpath+"/ldv_out/"+m_majorname+".txt";
		   //outDisf(ldv_dfile,2,m_pol,m_bufdist);
		   long endTime = System.currentTimeMillis();
		   myroadinf.clear();
		   myroadinf=null;
		   m_first_inf=null;
		   m_hdv_inf=null;
		   m_ldv_inf=null;
		   m_aadt_inf=null;
		   System.out.println("get traffic info：" + (m_altime) + "ms");
		   System.out.println("write txt ：" + (endTime - startTime) + "ms"); 
	  }
	   
		  public void SetDataFormat(int in_pgid, ArrayList<RoadInfo> in_myroadinf, double  in_x, double  in_y,float in_emission){
			  pgid=in_pgid;
			  myroadinf=in_myroadinf;
			  hloc_x=in_x;
			  hloc_y=in_y;
			  m_emission=in_emission; 
		  }
	   
	  ///type=1: hdv;type=2:ldv	  
	  public boolean DataFormat(){
		  	String first_inf="";
	    	String hdv_inf="";
	    	String ldv_inf="";
	    	String aadt_inf="";
	    	DecimalFormat df = new DecimalFormat("#.##");
	    	int i2=-1;
	    	for(int i=0;i<myroadinf.size();i++){
	    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
		    	if(myrd_inf.length<21){
	    			myroadinf.remove(i);
	    			i--;
	    		}
	    	}
  		
	    	for(int i=0;i<myroadinf.size();i++){
	    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
	    		//int _lanes=myrd_inf.lanes;
	    		int mywidth=8;
	    		if(myrd_inf.lanes>0){
	    			mywidth=(int)( 0.3048*12*myrd_inf.lanes);
	    			if (mywidth%2==1){
	    				mywidth=mywidth+1; 
	    			}
	    		}else{
		    	        if (myrd_inf.funccl>0){
			    	          if (myrd_inf.funccl==2 || myrd_inf.funccl==11){
			    	        	  mywidth=36;
			    	          }else if(myrd_inf.funccl==6 || myrd_inf.funccl==7 ||myrd_inf.funccl==17){
			    	        	  mywidth=8 ;
			    	          }else if (myrd_inf.funccl==14 || myrd_inf.funccl==12 || myrd_inf.funccl==16){
			    	        	  mywidth=14 ;
			    	          }else{
			    	        	  mywidth=8 ;       			    	        	  
			    	          }
		    	        }
	    		}
  			
	    		if( mywidth>=myrd_inf.length){
	    			mywidth=Math.round((float)myrd_inf.length)-1;
	    			if (mywidth%2==1){
	    				int mywidth1=mywidth+1;
	    				if(mywidth1<myrd_inf.length){
	    					mywidth=mywidth1; 
	    				}else{
	    					mywidth1=mywidth-1;
	    					if(mywidth1>0){
	    						mywidth=mywidth1;
	    					}
	    				}
	    			}
	    		}
	    		// postprocessing for the difference between width length 
	    		if((myrd_inf.length-mywidth)<1.0){
	    			mywidth=(int)Math.floor(myrd_inf.length)-1;
	    			if (mywidth%2==1){
	    				mywidth=mywidth-1;
	    				if(mywidth==0)
	    					mywidth=1; 
	    			}
	    		}
	    		first_inf=first_inf+"1 "+df.format(myrd_inf.start_x)+" "+df.format(myrd_inf.start_y)+"  "+
	    		   		  df.format(myrd_inf.end_x)+" "+df.format(myrd_inf.end_y)+
	    		  		   "  0 "+Integer.toString(mywidth)+" 0 0 0  "+myrd_inf.street_name+"\n";
	    		aadt_inf=aadt_inf+Integer.toString(myrd_inf.aadt)+" ";
	    		hdv_inf=hdv_inf+Integer.toString(myrd_inf.hdv)+" ";
	    		ldv_inf=ldv_inf+Integer.toString(myrd_inf.ldv)+" ";
	    		i2=i+1;
	    		if(i2 %10==0){
	    			aadt_inf=aadt_inf+"\n";
	    			hdv_inf=hdv_inf+"\n";
	    			ldv_inf=ldv_inf+"\n";
	    		}
	    	}
	    	if(i2%10>0){
	    		aadt_inf=aadt_inf+"\n";
	    		hdv_inf=hdv_inf+"\n";
  			    ldv_inf=ldv_inf+"\n";
	    	}
	    	String line4="10. 28. 0. 0.     1    "+Integer.toString(myroadinf.size())+"   1.  0 1 0.\n"+
	    		   Integer.toString(pgid)+"          "+df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n"+  
	    		   df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n";
	    	m_line4=line4 ;
			m_first_inf=first_inf;
			m_aadt_inf=aadt_inf;
			m_hdv_inf=hdv_inf;
			m_ldv_inf=ldv_inf;
			m_rdsum=myroadinf.size(); 
	        return true;
	  }
	  
	  //type=1: hdv; type=2: ldv; type=0 aadt 
	  public Boolean outDisf(String path,int type,String poltype, int bufdist){
			try{
				  BufferedWriter out = new BufferedWriter(new FileWriter(path));
				  out.write("R1  Caltrain traffic 2002 AADT on home location \n");
				  out.write(poltype+"  \n");
				  out.write(bufdist+"   \n");
				  out.write(m_line4);
				  out.write(m_first_inf);
				  out.write(((String[])m_meteoArr.get(0))[0]+"\n ");
				  if(type==0){
					  out.write(m_aadt_inf);  
				  }else if(type==1){
					  out.write(m_hdv_inf);  
				  }else if(type==2){
					  out.write(m_ldv_inf);  
				  }else{}  
				  int ilp=1;
				  if(m_emission>=0){
					  while (ilp<=m_rdsum){
						    out.write(Float.toString(m_emission)+"    ");
						    if (ilp%10==0) out.write("\n");
						    ilp++;
					  }
				  }else{
					  double _vs=30; 
					  for(int i=0;i<myroadinf.size();i++){
				    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
				    		int mywidth=8;
				    		if(myrd_inf.funccl==2 || myrd_inf.funccl==11|| myrd_inf.funccl==12){
				    			_vs=65;
				    		}else if(myrd_inf.funccl==6 || myrd_inf.funccl==7|| myrd_inf.funccl==16|| myrd_inf.funccl==17){
				    			_vs=30;
				    		}else if(myrd_inf.funccl==14){
				    			_vs=50;
				    		}
				    		double _log_ef=0.92*myrd_inf.fhdv+0.0089*_vs*1.609+13.64;
				    		double _emission=Math.pow(10,_log_ef)/Math.pow(10,12); 
				    		out.write(Math.round(_emission)+"    ");
						    if ((i+1)%10==0) out.write("\n");
					 }
				     ilp=myroadinf.size()+1; 
				  }
				  if ((ilp-1)%10>0) out.write("\n");
				  out.write(((String[])m_meteoArr.get(0))[1]+" \n");
                  for(int i=1;i<m_meteoArr.size();i++){
                	  String [] lines=(String[])m_meteoArr.get(i);
    				  out.write(lines[0]+"\n");
    				  out.write(lines[1]+"\n");                	  
                  }
			      out.close();
			   }catch(IOException e1) {
			        System.out.println("Error during reading/writing:"+path);
			   }
			return true;
		}
	  
	// ---------------------------------------
		  public boolean RealTrfDataFormat(){
			  	String first_inf="";
		    	String hdv_inf="";
		    	String ldv_inf="";
		    	String emStr=String.valueOf(m_emission);
		    	DecimalFormat df = new DecimalFormat("#.##");
		    	int i2=-1;
		    	for(int i=0;i<myroadinf.size();i++){
		    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
			    	if(myrd_inf.length<21){
		    			myroadinf.remove(i);
		    			i--;
		    		}
		    	}
		    	String emission_info="";
	        	int road_sz=myroadinf.size();
	        	String [] realtrf_Str=new String[m_meteoArr.size()]; 
	        	int[] missTfArr=new int[m_meteoArr.size()]; 
	        	for(int i=0;i<m_meteoArr.size();i++){
	        		realtrf_Str[i]="";
	        		missTfArr[i]=1;
	        	}
		    	for(int i=0;i<road_sz;i++){
		    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
		    		//int _lanes=myrd_inf.lanes;
		    		int mywidth=8;
		    		if(myrd_inf.lanes>0){
		    			mywidth=(int)( 0.3048*12*myrd_inf.lanes);
		    			if (mywidth%2==1){
		    				mywidth=mywidth+1; 
		    			}
		    		}else{
			    	        if (myrd_inf.funccl>0){
				    	          if (myrd_inf.funccl==2 || myrd_inf.funccl==11){
				    	        	  mywidth=36;
				    	          }else if(myrd_inf.funccl==6 || myrd_inf.funccl==7 ||myrd_inf.funccl==17){
				    	        	  mywidth=8 ;
				    	          }else if (myrd_inf.funccl==14 || myrd_inf.funccl==12 || myrd_inf.funccl==16){
				    	        	  mywidth=14 ;
				    	          }else{
				    	        	  mywidth=8 ;       			    	        	  
				    	          }
			    	        }
		    		}
		    		/// mywidth=8;
		    		if( mywidth>=myrd_inf.length){
		    			mywidth=Math.round((float)myrd_inf.length)-1;
		    			if (mywidth%2==1){
		    				int mywidth1=mywidth+1;
		    				if(mywidth1<myrd_inf.length){
		    					mywidth=mywidth1; 
		    				}else{
		    					mywidth1=mywidth-1;
		    					if(mywidth1>0){
		    						mywidth=mywidth1;
		    					}
		    				}
		    			}
		    		}
		    		// postprocessing for the difference between width length 
		    		if((myrd_inf.length-mywidth)<1.0){
		    			mywidth=(int)Math.floor(myrd_inf.length)-1;
		    			if (mywidth%2==1){
		    				mywidth=mywidth-1;
		    				if(mywidth==0)
		    					mywidth=1; 
		    			}
		    		}
		    		first_inf=first_inf+"1 "+df.format(myrd_inf.start_x)+" "+df.format(myrd_inf.start_y)+"  "+
		    		   		  df.format(myrd_inf.end_x)+" "+df.format(myrd_inf.end_y)+
		    		  		   "  0 "+Integer.toString(mywidth)+" 0 0 0  "+myrd_inf.street_name+"\n";
	    			emission_info+=emStr+" ";
	    			int[] myflow=myrd_inf.tflow;
	    			for(int j=0;j<myflow.length;j++){
	    				realtrf_Str[j]=realtrf_Str[j]+myflow[j]+" ";
	    				if(myflow[j]==-1&&missTfArr[j]==1){
	    					missTfArr[j]=0; 
	    				}
	    				if(i!=0 && (i+1) %10==0){
	    	    			realtrf_Str[j]=realtrf_Str[j]+"\n";    					
	    				}
	    			}    			
		    		if(i!=0 && (i+1) %10==0){
		    			emission_info=emission_info+"\n";
		    		}
		    	}
		    	if(road_sz %10>0){
		    		emission_info+=" \n"; 
		    		for(int j=0;j<realtrf_Str.length;j++){
	    				realtrf_Str[j]=realtrf_Str[j]+"\n";
		    		}
		          }
		    	
		    	String line4="10. 28. 0. 0.     1    "+Integer.toString(myroadinf.size())+"   1.  0 1 0.\n"+
			    		   Integer.toString(pgid)+"          "+df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n"+  
			    		   df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n";
		    	String combinedline="";
		    	int iVal=1;
		    	for(int i=0;i<m_meteoArr.size();i++){
			    	  if(missTfArr[i]==1){
				          String [] lines=(String[])m_meteoArr.get(i);
				          String lines0="11101     "+String.valueOf(iVal++)+"   "+lines[0];
			    		  combinedline+=lines0+"\n"; 
			    		  combinedline+=realtrf_Str[i];
			    		  combinedline+=emission_info;
			    		  combinedline+=lines[1]+"\n";
			    	  }
	            }
		    	m_line4=line4 ;
				m_first_inf=first_inf;
				m_combinedlines=combinedline ;
				//m_flow_inf=tflow_inf;
				//m_hdv_inf=hdv_inf;
				//m_ldv_inf=ldv_inf;
				//m_emission=emission;

		        return true;
		  }
		  
	  
	  // ---------------------------------------
	  public boolean RealTrfDataFormat_avg(){
		  	String first_inf="";
	    	String hdv_inf="";
	    	String ldv_inf="";
	    	String emStr=String.valueOf(m_emission);
	    	DecimalFormat df = new DecimalFormat("#.##");
	    	int i2=-1;
	    	for(int i=0;i<myroadinf.size();i++){
	    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
		    	if(myrd_inf.length<21){
	    			myroadinf.remove(i);
	    			i--;
	    		}
	    	}
	    	String emission_info="";
        	int road_sz=myroadinf.size();
        	String [] realtrf_Str=new String[m_meteoArr.size()];
        	int [] realtrf_avg=new int[road_sz];
        	String realtrf_avg_Str="";
        	int[] missTfArr=new int[m_meteoArr.size()]; 
        	for(int i=0;i<m_meteoArr.size();i++){
        		realtrf_Str[i]="";
        		missTfArr[i]=1;
        	}
	    	for(int i=0;i<road_sz;i++){
	    		RoadInfo myrd_inf=(RoadInfo) myroadinf.get(i);
	    		//int _lanes=myrd_inf.lanes;
	    		int mywidth=8;
	    		if(myrd_inf.lanes>0){
	    			mywidth=(int)( 0.3048*12*myrd_inf.lanes);
	    			if (mywidth%2==1){
	    				mywidth=mywidth+1; 
	    			}
	    		}else{
		    	        if (myrd_inf.funccl>0){
			    	          if (myrd_inf.funccl==2 || myrd_inf.funccl==11){
			    	        	  mywidth=36;
			    	          }else if(myrd_inf.funccl==6 || myrd_inf.funccl==7 ||myrd_inf.funccl==17){
			    	        	  mywidth=8 ;
			    	          }else if (myrd_inf.funccl==14 || myrd_inf.funccl==12 || myrd_inf.funccl==16){
			    	        	  mywidth=14 ;
			    	          }else{
			    	        	  mywidth=8 ;       			    	        	  
			    	          }
		    	        }
	    		}
	    		// mywidth=8;
	    		if( mywidth>=myrd_inf.length){
	    			mywidth=Math.round((float)myrd_inf.length)-1;
	    			if (mywidth%2==1){
	    				int mywidth1=mywidth+1;
	    				if(mywidth1<myrd_inf.length){
	    					mywidth=mywidth1; 
	    				}else{
	    					mywidth1=mywidth-1;
	    					if(mywidth1>0){
	    						mywidth=mywidth1;
	    					}
	    				}
	    			}
	    		}
	    		
	    		// postprocessing for the difference between width length 
	    		if((myrd_inf.length-mywidth)<1.0){
	    			mywidth=(int)Math.floor(myrd_inf.length)-1;
	    			if (mywidth%2==1){
	    				mywidth=mywidth-1;
	    				if(mywidth==0)
	    					mywidth=1; 
	    			}
	    		}
	    		
	    		first_inf=first_inf+"1 "+df.format(myrd_inf.start_x)+" "+df.format(myrd_inf.start_y)+"  "+
	    		   		  df.format(myrd_inf.end_x)+" "+df.format(myrd_inf.end_y)+
	    		  		   "  0 "+Integer.toString(mywidth)+" 0 0 0  "+myrd_inf.street_name+"\n";
    			emission_info+=emStr+" ";
    			int[] myflow=myrd_inf.tflow;
    			realtrf_avg[i]=0;
    			for(int j=0;j<myflow.length;j++){
    				realtrf_avg[i]=realtrf_avg[i]+myflow[j];
    				realtrf_Str[j]=realtrf_Str[j]+myflow[j]+" ";
    				if(myflow[j]==-1&&missTfArr[j]==1){
    					missTfArr[j]=0; 
    				}
    				if(i!=0 && (i+1) %10==0){
    	    			realtrf_Str[j]=realtrf_Str[j]+"\n";    					
    				}
    			}
    			realtrf_avg[i]=(int)(realtrf_avg[i]/(double)myflow.length);
    			realtrf_avg_Str+=String.valueOf(realtrf_avg[i])+" ";
	    		if(i!=0 && (i+1) %10==0){
	    			emission_info=emission_info+"\n";
	    			realtrf_avg_Str=realtrf_avg_Str+"\n";
	    		}
	    	}
	    	if(road_sz %10>0){
	    		emission_info+=" \n"; 
	    		realtrf_avg_Str+=" \n";
	    		for(int j=0;j<realtrf_Str.length;j++){
    				realtrf_Str[j]=realtrf_Str[j]+"\n";
	    		}
	          }
	    	
	    	String line4="10. 28. 0. 0.     1    "+Integer.toString(myroadinf.size())+"   1.  0 1 0.\n"+
		    		   Integer.toString(pgid)+"          "+df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n"+  
		    		   df.format(hloc_x)+"   "+df.format(hloc_y)+"    11 \n";
	    	String combinedline="";
	    	
	    	int iVal=1;
	    	boolean isfirst=true;
	    	for(int i=0;i<m_meteoArr.size();i++){
		    	  if(missTfArr[i]==1){
			          String [] lines=(String[])m_meteoArr.get(i);
			          String lines0="";
			          if(i==0){
			        	  lines0="11101     "+String.valueOf(iVal++)+"   "+lines[0];
			          }else{
			        	  lines0="10001     "+String.valueOf(iVal++)+"   "+lines[0];
			          }
		    		  combinedline+=lines0+"\n";
			          if(isfirst){
			        	  combinedline+=realtrf_avg_Str;
			        	  combinedline+=emission_info;
			        	  isfirst=false;
			          }
		    		  combinedline+=lines[1]+"\n";
		    	  }
            }
	    	m_line4=line4 ;
			m_first_inf=first_inf;
			m_combinedlines=combinedline ;
			//m_flow_inf=tflow_inf;
			//m_hdv_inf=hdv_inf;
			//m_ldv_inf=ldv_inf;
			//m_emission=emission;
	        return true;
	  }
	  
	  //type=1: hdv; type=2: ldv; type=0 aadt 
	  public Boolean realTrf_outDisf(String path,int type){
			try{
				  BufferedWriter out = new BufferedWriter(new FileWriter(path));
				  out.write("R1  realtime traffic flow on home location \n");
				  out.write("1CO   \n");
				  out.write("3000   \n");
				  out.write(m_line4);
				  out.write(m_first_inf);
				  if(type==0){
					  out.write(m_combinedlines);  
				  }else if(type==1){
					  out.write(m_hdv_inf);  
				  }else if(type==2){
					  out.write(m_ldv_inf);  
				  }else{}  
			      out.close();
			   }catch(IOException e1) {
			        System.out.println("Error during reading/writing:"+path);
			   }
			return true;
		}
	  
	  
	  
}
