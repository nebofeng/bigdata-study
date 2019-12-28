package pers.nebo.sparksql.loaddata2hive;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import pers.nebo.util.DateUtils;
import pers.nebo.util.StringUtils;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class Data2File {
	public static String MONITOR_FLOW_ACTION ="./monitor_flow_action";
	public static String MONITOR_CAMERA_INFO ="./monitor_camera_info";
	
	public static void main(String[] args) {
		CreateFile(MONITOR_FLOW_ACTION );
		CreateFile(MONITOR_CAMERA_INFO );
		System.out.println("running... ...");
		try {
			mock();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("finished");
	}
	
	/**
	 * 创建文件
	 * @param
	 */
	public static Boolean CreateFile(String pathFileName){
		try {
			File file = new File(pathFileName);
			if(file.exists()){
				file.delete();
			}
			boolean createNewFile = file.createNewFile();
			System.out.println("create file "+pathFileName+" success!");
			return createNewFile;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 向文件中写入数据
	 * @param
	 */
	public static void WriteDataToFile(String pathFileName,String newContent,FileOutputStream fos,OutputStreamWriter osw ,PrintWriter pw) throws FileNotFoundException, UnsupportedEncodingException {
		//产生一行模拟数据
		String content = newContent;
		File file = new File(pathFileName);
		fos=new FileOutputStream(file,true);
		osw=new OutputStreamWriter(fos, "UTF-8");
		pw =new PrintWriter(osw);
		pw.write(content+"\n");

	}
	
	
	
	//生成模拟数据
	public static void mock() throws IOException {

		/**
		 * 创建流对象
		 */
		FileOutputStream fos = null ;
		OutputStreamWriter osw = null;
		PrintWriter pw = null ;

		File file = new File(MONITOR_FLOW_ACTION);//monitor_flow_action
		fos=new FileOutputStream(file,true);
		osw=new OutputStreamWriter(fos, "UTF-8");
		pw =new PrintWriter(osw);


		List<Row> dataList = new ArrayList<Row>();
    	Random random = new Random();
    	
    	String[] locations = new String[]{"鲁","京","京","京","沪","京","京","深","京","京"}; 
    	String date = DateUtils.getTodayDate();
    	
    	/**
    	 * 模拟3000个车辆
    	 */
    	for (int i = 0; i < 3000; i++) {
        	String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+ StringUtils.fulfuill(5,random.nextInt(100000)+"");
        	
        	//baseActionTime 模拟24小时
        	String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");
        	/**
        	 * 这里的for循环模拟每辆车经过不同的卡扣不同的摄像头 数据。
        	 */
        	for(int j = 0 ; j < random.nextInt(300)+1 ; j++){
        		//模拟每个车辆每被30个摄像头拍摄后 时间上累计加1小时。这样做使数据更加真实。
				if(j % 30 == 0 && j != 0){
					Integer addOneHourTime = Integer.parseInt(baseActionTime.split(" ")[1])+1;
					if(addOneHourTime == 24){
						addOneHourTime = 0;
					}
					baseActionTime = date + " " + StringUtils.fulfuill(addOneHourTime+"");
				}
        		
        		String actionTime = baseActionTime + ":" 
        				+ StringUtils.fulfuill(random.nextInt(60)+"") + ":" 
        				+ StringUtils.fulfuill(random.nextInt(60)+"");//模拟经过此卡扣开始时间 ，如：2019-07-23 20:09:10
        		
        		String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");//模拟9个卡扣monitorId
        		
        		String speed = random.nextInt(260)+1+"";//模拟速度
        		
        		String roadId = random.nextInt(50)+1+"";//模拟道路id 【1~50 个道路】
        		
        		String cameraId = StringUtils.fulfuill(5, random.nextInt(100000)+"");//模拟摄像头id cameraId
        		
        		String areaId = StringUtils.fulfuill(2,random.nextInt(8)+1+"");//模拟areaId 【一共8个区域】
        		
        		
        		//将数据写入到文件中
        		String content = date+"\t"+monitorId+"\t"+cameraId+"\t"+car+"\t"+actionTime+"\t"+speed+"\t"+roadId+"\t"+areaId;
				/**
				 * 将以上一行数据写入文件中 monitor_flow_action文件中
				 */
				pw.write(content+"\n");

				Row row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId);
        		dataList.add(row);
        	}
		}
    	pw.flush();
    	/**
    	 * 生成 monitor_id 对应camera_id表
    	 */
    	Map<String,Set<String>> monitorAndCameras = new HashMap<>();
    	
    	int index = 0;
    	for(Row row : dataList){
    		//row.getString(1) monitor_id
    		Set<String> sets = monitorAndCameras.get(row.getString(1));
    		if(sets == null){
    			sets = new HashSet<>();
    			monitorAndCameras.put((String)row.getString(1), sets);
    		}
    		index++;
    		//这里每隔1000条数据随机插入一条数据，模拟出来标准表中卡扣对应摄像头的数据。这个摄像头的数据不一定会在车辆数据中有。
    		if(index % 5000 == 0){
    			sets.add(StringUtils.fulfuill(5, random.nextInt(100000)+""));
    		} 
    		//row.getString(2) camera_id
    		sets.add(row.getString(2)); 
    	}

		/**
		 * 创建对象
		 */
		File file2 = new File(MONITOR_CAMERA_INFO);//monitor_camera_info
		fos=new FileOutputStream(file2,true);
		osw=new OutputStreamWriter(fos, "UTF-8");
		pw =new PrintWriter(osw);

    	Set<Entry<String,Set<String>>> entrySet = monitorAndCameras.entrySet();
    	for (Entry<String, Set<String>> entry : entrySet) {
    		String monitor_id = entry.getKey();
    		Set<String> sets = entry.getValue();
    		for (String cameraId : sets) {
    			//将数据写入到文件
    			String content = monitor_id+"\t"+cameraId;
				/**
				 * 将以上一行数据写入文件中
				 */
				//产生一行模拟数据
				pw.write(content+"\n");
			}
		}
		pw.flush();
		/**
		 * 关闭流对象
		 */
		//注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
		pw.close();
		osw.close();
		fos.close();
	}
}
