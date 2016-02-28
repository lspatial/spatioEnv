package ucla_caline4_sub;

import java.sql.SQLException;

public class DisAADTMain3 {

	/**
	 * @param args   	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        System.out.println(Thread.currentThread().getName() + " 线程运行开始!");
        String fpath="/mnt/quickDB/out_files/ralph_dis/aadtnonfrw/1500";  
        new DisAADTTrd("node1",1,3,fpath).start(); 
        System.out.println(Thread.currentThread().getName() + " 线程运行结束!");
	}
}
	
class DisAADTTrd extends Thread {
   int m_threadNo=0;
   int m_totalThreads=1;
   String       m_fpath="";  
   
   public DisAADTTrd(String threadName, int cuThreadNo,int titalThreads,String fpath) {
        super(threadName);
        m_threadNo=cuThreadNo;
        m_totalThreads=titalThreads;
        m_fpath=fpath; 
   }
   

   public void run() {
	   String myname=getName();
	   System.out.println(myname+ " 线程运行开始 ... ... ");
	   if(myname.equals("node1")){
		   AADTRealTrCls mynode1=new AADTRealTrCls();
			  try{
				  mynode1.initDBLink("jdbc:postgresql://localhost:6432/real_trf","postgres","lfr524");
				 
				  mynode1.extractSingleAADT("uclacal4_subjects_500_frways","aadt02_frways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/CO/aadtfrw/500",500,"1CO",10);
				  mynode1.extractSingleAADT("uclacal4_subjects_1500_frways","aadt02_frways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/CO/aadtfrw/1500",1500,"1CO",10);
				  mynode1.extractSingleAADT("uclacal4_subjects_500_nofrways","aadt02_m_nofrways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/CO/aadtnonfrw/500",500,"1CO",10); 
				  mynode1.extractSingleAADT("uclacal4_subjects_1500_nofrways","aadt02_m_nofrways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/CO/aadtnonfrw/1500",1500,"1CO",10);

				  // mynode1.extractSingleAADT("uclacal4_subjects_500_frways","aadt02_frways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/UFP/aadtfrw/500",500,"6UFP",-1);
				  // mynode1.extractSingleAADT("uclacal4_subjects_1500_frways","aadt02_frways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/UFP/aadtfrw/1500",1500,"6UFP",-1);
				  // mynode1.extractSingleAADT("uclacal4_subjects_500_nofrways","aadt02_m_nofrways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/UFP/aadtnonfrw/500",500,"6UFP",-1); 
				  // mynode1.extractSingleAADT("uclacal4_subjects_1500_nofrways","aadt02_m_nofrways","/home/samba/shared_data/Calines4Files/UCLA/pollutants/UFP/aadtnonfrw/1500",1500,"6UFP",-1);

				  mynode1.closeDbLink();
			  }catch(SQLException e){
				  e.printStackTrace();
				  e.getNextException();
			  }
		}else{
			  System.out.println(getName() + " No pgsql node!!");
		}
	   System.out.println(getName() + " 线程运行结束!");
  }
}	