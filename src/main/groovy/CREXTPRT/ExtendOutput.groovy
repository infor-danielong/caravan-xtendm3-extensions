/**
*  @Name: CREXTPRT.ExtendOutput
*  @Description: ExtendOutput for MWS435PF, MMS480PF/MWS620PF
*  @Authors: Nixon Power Ong
*/

/**
* CHANGELOGS
* Version    Date    User               Description
* 1.0.0      240315  Nixon Power Ong    Initial Release
* 1.0.1      240508  Nixon Power Ong    After XtendM3 review
*/

import java.util.*;
import java.util.regex.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ExtendOutput extends ExtendM3Trigger {
  private final MethodAPI method;
  private final ProgramAPI program;
  private final DatabaseAPI database;
  private final LoggerAPI logger;
  private final MICallerAPI miCaller;
  
  int MAX_RECORDS = 10000;
  
  public ExtendOutput(MethodAPI method, ProgramAPI program, DatabaseAPI database, LoggerAPI logger, MICallerAPI miCaller) {
	  this.program = program;
    this.database = database;
    this.method = method;
    this.logger = logger;
    this.miCaller = miCaller;
  }
  
  public void main() {
    String printerFile = method.getArgument(0) as String;
    String jobNumber = method.getArgument(1) as String;
    String structure = method.getArgument(2) as String;
    int variant = method.getArgument(3) as int;
    int rpbk = method.getArgument(4) as int;
    HashMap<String, Object> fieldMap = method.getArgument(5) as HashMap;
    
    logger.debug("@@ExtendOutput is running");

    if(printerFile.equals("MMS480PF") || printerFile.equals("MWS620PF")) {
      if (structure.equals("CRV_03-01")) {
        if (rpbk == 6) {
          startMMS480PFv3(fieldMap.get("OQCONO") as int,
                          fieldMap.get("OQINOU") as int,
                          fieldMap.get("OQCONN") as int,
                          fieldMap.get("&SIZE") as int,
                          fieldMap.get("OQDLIX") as long);
                          
          startMWS435PFv1(fieldMap.get("OQCONO") as int,
                          fieldMap.get("OQINOU") as int,
                          fieldMap.get("OQCONN") as int,
                          fieldMap.get("&SIZE") as int,
                          fieldMap.get("OQDLIX") as long);
        }
      }
    } else if(printerFile.equals("MWS435PF")) {
      if (structure.equals("CRV_01-01")) {
        if (rpbk == 6) {
          startMWS435PFv1(fieldMap.get("OQCONO") as int,
                          fieldMap.get("OQINOU") as int,
                          fieldMap.get("OQCONN") as int,
                          fieldMap.get("&SIZE") as int,
                          fieldMap.get("OQDLIX") as long);
        }
      }
    }
  }
  
  /**
   * Extend output for MWS435PF variant 1
   * @params CONO - Input company
   * @params INOU - Input direction
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
   */
  void startMWS435PFv1(int CONO, int INOU, int CONN, int ROW_SIZE, long DLIX) {
    deleteRecordsByCONN(CONO, CONN, "EXT434");
    deleteRecordsByCONN(CONO, CONN, "EXT435");
    deleteRecordsByCONN(CONO, CONN, "EXT436");
    
    deleteRecordsByDLIX(CONO, DLIX, "EXTD34");
    deleteRecordsByDLIX(CONO, DLIX, "EXTD35");
    deleteRecordsByDLIX(CONO, DLIX, "EXTD36");
    
    DBAction query = database.table("MHDISH").index("20").selection("OQCONO", "OQINOU", "OQCONN", "OQDLIX", "OQCONA", "OQPGRS").build();
    DBContainer container = query.createContainer();
    container.set("OQCONO", CONO);
    container.set("OQINOU", INOU);
    container.set("OQCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    ArrayList<Map<String, String>> DLIX_LINES = new ArrayList<Map<String, String>>();
    
    DBAction queryMHDISL = database.table("MHDISL").index("00").selection("URCONO", "URDLIX", "URITNO", "URRIDN", "URRIDL").build();
    DBContainer containerMHDISL = queryMHDISL.createContainer();
    
    DBAction queryMITMAH = database.table("MITMAH").index("00").selection("HMTY15", "HMTX15").build();
    DBContainer containerMITMAH = queryMITMAH.createContainer();
    
    DBAction queryEXTDEL = database.table("EXTDEL").index("00").build();
    DBContainer containerEXTDEL = queryEXTDEL.createContainer();
    containerEXTDEL.set("EXCONO", CONO);
    
    query.readAll(container, 3, MAX_RECORDS, { DBContainer mhdishData ->
      Map result = new HashMap(); 
      
      long lineDLIX = mhdishData.get("OQDLIX") as long;
      String CONA = mhdishData.get("OQCONA");
      String OQPGRS =  mhdishData.get("OQPGRS");
      
      containerMHDISL.set("URCONO", CONO);
      containerMHDISL.set("URDLIX", lineDLIX);
      int ctr = 1;
      int pageNum = 1;
      
      containerEXTDEL.set("EXDLIX", lineDLIX);
      
      if(!queryEXTDEL.read(containerEXTDEL)) {
        queryMHDISL.readAll(containerMHDISL, 2, MAX_RECORDS, { DBContainer mhdislData ->
          Map resultLine = new HashMap(); 
          
          // Converts ITNO to a sortable integer (ex. 1011234 1 -> 101123401, 1011234 22 -> 101123422)
          String ITNO = mhdislData.get("URITNO");
          String ITN = ITNO.replaceAll("\\s+", " ");
          String[] itnos = ITN.split(" ");
          String numString = "";
          String ITN2 = "";
          if(itnos.length > 1) {
            if(itnos[1].isNumber()){
              int num = itnos[1] as int;
              
              if(num < 10) {
                numString = "0" + num;
              } else {
                numString = num;
              }
              ITN2 = itnos[0] + numString;
            } else {
              ITN2 = ITN;
            }
          } else {
            ITN2 = ITNO;
          }
           
        
          String blockNo = getBlock(CONO, ITNO);
          String color = "";
          String size = "";
          
          containerMITMAH.set("HMCONO", CONO);
          containerMITMAH.set("HMITNO", ITNO);
          
          if (queryMITMAH.read(containerMITMAH)) {
            color = containerMITMAH.get("HMTY15");
            size = containerMITMAH.get("HMTX15");
          }
          
          resultLine.put("DLIX", lineDLIX);
          resultLine.put("CONA", CONA.trim());
          resultLine.put("ORNO", mhdislData.get("URRIDN"));
          resultLine.put("PONR", mhdislData.get("URRIDL") as String);
          resultLine.put("ITNO", ITNO);
          resultLine.put("ITN2", ITN2);
          resultLine.put("CFI4", blockNo);
          resultLine.put("COLR", color);
          resultLine.put("SIZE", size);
        
          if(blockNo.isNumber()) {
            int block = blockNo as int;
            
            if(block <= 3999) {
              resultLine.put("TYPE", "1");
            } else {
              resultLine.put("TYPE", "2");
            }
          } else {
              resultLine.put("TYPE", "0");
          }
          
          if(OQPGRS.equals("50")) {
            LINES.add(resultLine);
            
            if(DLIX == lineDLIX) {
              DLIX_LINES.add(resultLine);
              
              containerEXTDEL.set("EXDLIX", lineDLIX);
              containerEXTDEL.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
              containerEXTDEL.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
              containerEXTDEL.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
              containerEXTDEL.set("EXCHNO", 0);
              containerEXTDEL.set("EXCHID", program.getUser());
              
              if(!program.getUser().equals("MOVEX")) {
                queryEXTDEL.insert(containerEXTDEL);
              }
            }
          }
        });
      }  
    });
      
    insertDataToEXT436(CONO, CONN, LINES);
    insertDataToEXTD36(CONO, DLIX, DLIX_LINES);
    
    processSortedDataAll(CONO, CONN, ROW_SIZE);
    processSortedDataSingle(CONO, DLIX, ROW_SIZE);
    
    computeTotalPageEXT434(CONO, CONN);
    computeTotalPageEXTD34(CONO, DLIX);
  }
  
  /**
   * Get total number of pages for EXT434
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @return nothing
  */
  void computeTotalPageEXT434(int CONO, int CONN) {
    DBAction query = database.table("EXT434").index("00").selection("EXCONA", "EXTYPE", "EXPAGE").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    ArrayList<Map<String, String>> CONA_LINES = new ArrayList<Map<String, String>>();
   
    String currCONA = null;
    int ctr = 0;
   
    query.readAll(container, 2, MAX_RECORDS, { DBContainer data ->
      Map result = new HashMap(); 
      
      String CONA = data.get("EXCONA");
      
      if(currCONA == null) {
        currCONA = CONA;
      }
      
      if(!currCONA.equals(CONA)) {
        updateMaxPageEXT434(CONO, CONN, currCONA, ctr);
        ctr = 0;
        CONA_LINES.clear();
        currCONA = CONA;
      } 
      
      result.put("CONA", data.get("EXCONA"));
      result.put("TYPE", data.get("EXTYPE") as String);
      result.put("PAGE", data.get("EXPAGE") as String);
      
      CONA_LINES.add(result);
      
      ctr++;
    });
    logger.debug("@@ " + CONN + " " + currCONA + " " + ctr + " ");
    updateMaxPageEXT434(CONO, CONN, currCONA, ctr);
  }
  
  /**
   * Update MXPG, DPGN in EXT434 Table
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @params CONA - Input consignee number
   * @params val - Input total pages
   * @return nothing
  */
  void updateMaxPageEXT434(int CONO, int CONN, String CONA, int val) {
    DBAction query = database.table("EXT434").index("00").build();
    DBContainer container = query.getContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    container.set("EXCONA", CONA);
    
    int ctr = 1;
    
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("EXMXPG", val);
      lockedResult.set("EXDPGN", ctr);
      lockedResult.set("EXCHNO", (int)lockedResult.get("EXCHNO") + 1);
      lockedResult.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lockedResult.set("EXCHID", program.getUser());
      
      lockedResult.update();
      ctr++;
    };
    
    query.readAllLock(container, 3, updateCallBack);
  }
  
  /**
   * Get total number of pages for EXT434
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @return nothing
  */
  void computeTotalPageEXTD34(int CONO, long DLIX) {
    DBAction query = database.table("EXTD34").index("00").selection("EXCONA", "EXTYPE", "EXPAGE").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXDLIX", DLIX);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    ArrayList<Map<String, String>> CONA_LINES = new ArrayList<Map<String, String>>();
   
    String currCONA = null;
    int ctr = 0;
   
    query.readAll(container, 2, MAX_RECORDS, { DBContainer data ->
      Map result = new HashMap(); 
      
      String CONA = data.get("EXCONA");
      
      if(currCONA == null) {
        currCONA = CONA;
      }
      
      if(!currCONA.equals(CONA)) {
        updateMaxPageEXTD34(CONO, DLIX, currCONA, ctr);
        ctr = 0;
        CONA_LINES.clear();
        currCONA = CONA;
      } 
      
      result.put("CONA", data.get("EXCONA"));
      result.put("TYPE", data.get("EXTYPE") as String);
      result.put("PAGE", data.get("EXPAGE") as String);
      
      CONA_LINES.add(result);
      
      ctr++;
    });
    logger.debug("@@ " + DLIX + " " + currCONA + " " + ctr + " ");
    updateMaxPageEXTD34(CONO, DLIX, currCONA, ctr);
  }
  
  /**
   * Update MXPG, DPGN in EXT434 Table
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @params CONA - Input consignee number
   * @params val - Input total pages
   * @return nothing
  */
  void updateMaxPageEXTD34(int CONO, long DLIX, String CONA, int val) {
    DBAction query = database.table("EXTD34").index("00").build();
    DBContainer container = query.getContainer();
    container.set("EXCONO", CONO);
    container.set("EXDLIX", DLIX);
    container.set("EXCONA", CONA);
    
    int ctr = 1;
    
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("EXMXPG", val);
      lockedResult.set("EXDPGN", ctr);
      lockedResult.set("EXCHNO", (int)lockedResult.get("EXCHNO") + 1);
      lockedResult.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lockedResult.set("EXCHID", program.getUser());
      
      lockedResult.update();
      ctr++;
    };
    
    query.readAllLock(container, 3, updateCallBack);
  }
  
  /**
   * Process sorted data from EXT436 and compute page number
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
  */
  void processSortedDataAll(int CONO, int CONN, int ROW_SIZE) {
    DBAction sortedQuery = database.table("EXT436").index("00").selectAllFields().build();
    DBContainer sortedContainer = sortedQuery.createContainer();
    sortedContainer.set("EXCONO", CONO);
    sortedContainer.set("EXCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    int ctr = 1;
    int pageNum = 1;
    
    String currCONA = null;
    int currType = -1;
    
    DBAction lineQuery = database.table("EXT435").index("00").build();
    DBContainer lineContainer = lineQuery.createContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXCONN", CONN);
    
    DBAction headQuery = database.table("EXT434").index("00").build();
    DBContainer headContainer = headQuery.getContainer();
    headContainer.set("EXCONO", CONO);
    headContainer.set("EXCONN", CONN);
      
    sortedQuery.readAll(sortedContainer, 2, MAX_RECORDS, { DBContainer sortedData ->
      String CONA = sortedData.get("EXCONA");
      int TYPE = sortedData.get("EXTYPE");
      
      if(currCONA == null && currType == -1) {
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(!currCONA.equals(CONA) || (currCONA.equals(CONA) && currType != TYPE)) {
        ctr = 1;
        pageNum = 1;
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(ctr > ROW_SIZE) {
        ctr = 1;
        pageNum++;
      }
      
      lineContainer.set("EXORNO", sortedData.get("EXORNO"));
      lineContainer.set("EXPONR", sortedData.get("EXPONR"));
      lineContainer.set("EXDLIX", sortedData.get("EXDLIX"));
      lineContainer.set("EXCONA", sortedData.get("EXCONA"));
      lineContainer.set("EXITNO", sortedData.get("EXITNO"));
      lineContainer.set("EXITN2", sortedData.get("EXITN2"));
      lineContainer.set("EXCFI4", sortedData.get("EXCFI4"));
      lineContainer.set("EXTYPE", sortedData.get("EXTYPE"));
      lineContainer.set("EXCOLR", sortedData.get("EXCOLR"));
      lineContainer.set("EXSIZE", sortedData.get("EXSIZE"));
      lineContainer.set("EXPAGE", pageNum);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      headContainer.set("EXCONA", sortedData.get("EXCONA"));
      headContainer.set("EXTYPE", sortedData.get("EXTYPE"));
      headContainer.set("EXPAGE", pageNum);
      
      headContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      headContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXCHNO", 0);
      headContainer.set("EXCHID", program.getUser());
      
      lineQuery.insert(lineContainer);
      headQuery.insert(headContainer);
      
      ctr++;
    });
  }
  
  /**
   * Process sorted data from EXT436 and compute page number
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
  */
  void processSortedDataSingle(int CONO, long DLIX, int ROW_SIZE) {
    DBAction sortedQuery = database.table("EXTD36").index("00").selectAllFields().build();
    DBContainer sortedContainer = sortedQuery.createContainer();
    sortedContainer.set("EXCONO", CONO);
    sortedContainer.set("EXDLIX", DLIX);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    int ctr = 1;
    int pageNum = 1;
    
    String currCONA = null;
    int currType = -1;
    
    DBAction lineQuery = database.table("EXTD35").index("00").build();
    DBContainer lineContainer = lineQuery.createContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXDLIX", DLIX);
    
    DBAction headQuery = database.table("EXTD34").index("00").build();
    DBContainer headContainer = headQuery.getContainer();
    headContainer.set("EXCONO", CONO);
    headContainer.set("EXDLIX", DLIX);
      
    sortedQuery.readAll(sortedContainer, 2, MAX_RECORDS, { DBContainer sortedData ->
      String CONA = sortedData.get("EXCONA");
      int TYPE = sortedData.get("EXTYPE");
      
      if(currCONA == null && currType == -1) {
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(!currCONA.equals(CONA) || (currCONA.equals(CONA) && currType != TYPE)) {
        ctr = 1;
        pageNum = 1;
        currCONA = CONA;
        currType = TYPE;
      }
      
      if(ctr > ROW_SIZE) {
        ctr = 1;
        pageNum++;
      }
      
      lineContainer.set("EXORNO", sortedData.get("EXORNO"));
      lineContainer.set("EXPONR", sortedData.get("EXPONR"));
      lineContainer.set("EXDLIX", sortedData.get("EXDLIX"));
      lineContainer.set("EXCONA", sortedData.get("EXCONA"));
      lineContainer.set("EXITNO", sortedData.get("EXITNO"));
      lineContainer.set("EXITN2", sortedData.get("EXITN2"));
      lineContainer.set("EXCFI4", sortedData.get("EXCFI4"));
      lineContainer.set("EXTYPE", sortedData.get("EXTYPE"));
      lineContainer.set("EXCOLR", sortedData.get("EXCOLR"));
      lineContainer.set("EXSIZE", sortedData.get("EXSIZE"));
      lineContainer.set("EXPAGE", pageNum);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      headContainer.set("EXCONA", sortedData.get("EXCONA"));
      headContainer.set("EXTYPE", sortedData.get("EXTYPE"));
      headContainer.set("EXPAGE", pageNum);
      
      headContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      headContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXCHNO", 0);
      headContainer.set("EXCHID", program.getUser());
      
      lineQuery.insert(lineContainer);
      headQuery.insert(headContainer);
      
      ctr++;
    });
  }
  
  /**
   * Insert data to the EXT436 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
  */
  void insertDataToEXT436(int CONO, int CONN, ArrayList<Map<String, String>> LINES) {
    DBAction lineQuery = database.table("EXT436").index("00").build();
    DBContainer lineContainer = lineQuery.getContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXCONN", CONN);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      lineContainer.set("EXDLIX", record.DLIX as long);
      lineContainer.set("EXCONA", record.CONA);
      lineContainer.set("EXORNO", record.ORNO);
      lineContainer.set("EXPONR", record.PONR as int);
      lineContainer.set("EXITNO", record.ITNO);
      lineContainer.set("EXITN2", record.ITN2);
      lineContainer.set("EXCFI4", record.CFI4);
      lineContainer.set("EXTYPE", record.TYPE as int);
      lineContainer.set("EXCOLR", record.COLR);
      lineContainer.set("EXSIZE", record.SIZE);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      lineQuery.insert(lineContainer);
    }
  }
  
  /**
   * Insert data to the EXT436 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
  */
  void insertDataToEXTD36(int CONO, long DLIX, ArrayList<Map<String, String>> LINES) {
    DBAction lineQuery = database.table("EXTD36").index("00").build();
    DBContainer lineContainer = lineQuery.getContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXDLIX", DLIX);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      lineContainer.set("EXCONA", record.CONA);
      lineContainer.set("EXORNO", record.ORNO);
      lineContainer.set("EXPONR", record.PONR as int);
      lineContainer.set("EXITNO", record.ITNO);
      lineContainer.set("EXITN2", record.ITN2);
      lineContainer.set("EXCFI4", record.CFI4);
      lineContainer.set("EXTYPE", record.TYPE as int);
      lineContainer.set("EXCOLR", record.COLR);
      lineContainer.set("EXSIZE", record.SIZE);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      lineQuery.insert(lineContainer);
    }
  }
  
  
  /**
   * Get MMCFI4 - Block number from MITMAS
   * @params CONO - Input company
   * @params ITNO - Input item number
   * @return block
  */
  String getBlock(int CONO, String ITNO) {
    DBAction query = database.table("MITMAS").index("00").selection("MMCFI4").build();
    DBContainer container = query.createContainer();
    container.set("MMCONO", CONO);
    container.set("MMITNO", ITNO);
    
    String block = "";
    
    if (query.read(container)) {
      block = container.get("MMCFI4");
    }
    
    return block;
  }
  
  /**
   * Extend output for MMS480PF variant 3
   * @params CONO - Input company
   * @params INOU - Input direction
   * @params CONN - Input shipment number
   * @params ROW_SIZE - Input row size
   * @return nothing
  */
  void startMMS480PFv3(int CONO, int INOU, int CONN, int ROW_SIZE, long DLIX) {
    deleteRecordsByCONN(CONO, CONN, "EXT479");
    deleteRecordsByCONN(CONO, CONN, "EXT480");
    
    DBAction query = database.table("MHDISH").index("20").selection("OQCONO", "OQINOU", "OQCONN", "OQDLIX", "OQCONA").build();
    DBContainer container = query.createContainer();
    container.set("OQCONO", CONO);
    container.set("OQINOU", INOU);
    container.set("OQCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    ArrayList<Map<String, String>> DLIX_LINES = new ArrayList<Map<String, String>>();
    
    DBAction queryMHDISL = database.table("MHDISL").index("00").selection("URCONO", "URDLIX", "URITNO", "URRIDN", "URRIDL").build();
    DBContainer containerMHDISL = queryMHDISL.createContainer();
    
    query.readAll(container, 3, MAX_RECORDS, { DBContainer mhdishData ->
      Map result = new HashMap(); 
      
      long lineDLIX = mhdishData.get("OQDLIX") as long;
      String CONA = mhdishData.get("OQCONA");
      
      containerMHDISL.set("URCONO", CONO);
      containerMHDISL.set("URDLIX", lineDLIX);
      
      int ctr = 1;
      int pageNum = 1;
    
      queryMHDISL.readAll(containerMHDISL, 2, MAX_RECORDS, { DBContainer mhdislData ->
        Map resultLine = new HashMap(); 
        
        if(ctr > ROW_SIZE) {
          ctr = 1;
          pageNum++;
        }
        
        resultLine.put("DLIX", mhdishData.get("OQDLIX") as String);
        resultLine.put("CONA", CONA.trim());
        resultLine.put("ORNO", mhdislData.get("URRIDN"));
        resultLine.put("PONR", mhdislData.get("URRIDL") as String);
        resultLine.put("ITNO", mhdislData.get("URITNO"));
        resultLine.put("PAGE", pageNum);
        
        LINES.add(resultLine);
        
        if(DLIX == lineDLIX) {
          DLIX_LINES.add(resultLine);
        }
        
        ctr++;
      });
      
    });
   
    insertDataToEXT480(CONO, CONN, LINES);
    insertDataToEXTD80(CONO, DLIX, DLIX_LINES);
    
    computeTotalPageEXT479(CONO, CONN);
    computeTotalPageEXTD79(CONO, DLIX);
  }
  
  /**
   * Get total number of pages for EXT479
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @return nothing
  */
  void computeTotalPageEXT479(int CONO, int CONN) {
    DBAction query = database.table("EXT479").index("00").selection("EXDLIX", "EXCONA", "EXTYPE", "EXPAGE").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    ArrayList<Map<String, String>> DLIX_LINES = new ArrayList<Map<String, String>>();
   
    long currDLIX = -1;
    int ctr = 0;
   
    query.readAll(container, 2, MAX_RECORDS, { DBContainer data ->
      Map result = new HashMap(); 
      
      long DLIX = data.get("EXDLIX");
      
      if(currDLIX == -1) {
        currDLIX = DLIX;
      }
      
      if(currDLIX != DLIX) {
        updateMaxPageEXT479(CONO, CONN, currDLIX, ctr);
        ctr = 0;
        DLIX_LINES.clear();
        currDLIX = DLIX;
      } 
      
      result.put("DLIX", data.get("EXDLIX") as String);
      result.put("CONA", data.get("EXCONA"));
      result.put("TYPE", data.get("EXTYPE") as String);
      result.put("PAGE", data.get("EXPAGE") as String);
      
      DLIX_LINES.add(result);
      
      ctr++;
    });
    
    updateMaxPageEXT479(CONO, CONN, currDLIX, ctr);
  }
  
  /**
   * Update MXPG, DPGN in EXT479 Table
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @params DLIX - Input delivery number
   * @params val - Input total pages
   * @return nothing
  */
  void updateMaxPageEXT479(int CONO, int CONN, long DLIX, int val) {
    DBAction query = database.table("EXT479").index("00").build();
    DBContainer container = query.getContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    container.set("EXDLIX", DLIX);
    
    int ctr = 1;
    
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("EXMXPG", val);
      lockedResult.set("EXDPGN", ctr);
      lockedResult.set("EXCHNO", (int)lockedResult.get("EXCHNO") + 1);
      lockedResult.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lockedResult.set("EXCHID", program.getUser());
      
      lockedResult.update();
      ctr++;
    };
    
    query.readAllLock(container, 3, updateCallBack);
  }
  
  /**
   * Get total number of pages for EXT479
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @return nothing
  */
  void computeTotalPageEXTD79(int CONO, long DLIX) {
    DBAction query = database.table("EXTD79").index("00").selection("EXDLIX", "EXCONA", "EXTYPE", "EXPAGE").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXDLIX", DLIX);
    
    ArrayList<Map<String, String>> LINES = new ArrayList<Map<String, String>>();
    
    ArrayList<Map<String, String>> DLIX_LINES = new ArrayList<Map<String, String>>();
   
    long currDLIX = -1;
    int ctr = 0;
   
    query.readAll(container, 2, MAX_RECORDS, { DBContainer data ->
      Map result = new HashMap(); 
      
      long lineDLIX = data.get("EXDLIX");
      
      if(currDLIX == -1) {
        currDLIX = lineDLIX;
      }
      
      if(currDLIX != lineDLIX) {
        updateMaxPageEXTD79(CONO, currDLIX, ctr);
        ctr = 0;
        DLIX_LINES.clear();
        currDLIX = lineDLIX;
      } 
      
      result.put("CONA", data.get("EXCONA"));
      result.put("TYPE", data.get("EXTYPE") as String);
      result.put("PAGE", data.get("EXPAGE") as String);
      
      DLIX_LINES.add(result);
      
      ctr++;
    });
    
    updateMaxPageEXTD79(CONO, DLIX, ctr);
  }
  
  /**
   * Update MXPG, DPGN in EXT479 Table
   * @params CONO - Input CONO
   * @params CONN - Input shipment number
   * @params DLIX - Input delivery number
   * @params val - Input total pages
   * @return nothing
  */
  void updateMaxPageEXTD79(int CONO, long DLIX, int val) {
    DBAction query = database.table("EXT479").index("00").build();
    DBContainer container = query.getContainer();
    container.set("EXCONO", CONO);
    container.set("EXDLIX", DLIX);
    
    int ctr = 1;
    
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      lockedResult.set("EXMXPG", val);
      lockedResult.set("EXDPGN", ctr);
      lockedResult.set("EXCHNO", (int)lockedResult.get("EXCHNO") + 1);
      lockedResult.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lockedResult.set("EXCHID", program.getUser());
      
      lockedResult.update();
      ctr++;
    };
    
    query.readAllLock(container, 3, updateCallBack);
  }
  
  /**
   * Check CONA-Consignee if starts with 2301 or 2302
   * @params CONA - Input consignee
   * @params CONN - Input shipment number
   * @return boolean
  */
  boolean checkCONA(String CONA) {
    if(CONA.length() >= 4) {
      char[] ch = CONA.toCharArray();
      if(ch[0] == '2' && ch[1] == '3' && ch[2] == '0' && (ch[3] == '1' || ch[3] == '2')) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
  
  /**
   * Insert data to the EXT480 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
  */
  void insertDataToEXT480(int CONO, int CONN, ArrayList<Map<String, String>> LINES) {
    DBAction lineQuery = database.table("EXT480").index("00").build();
    DBContainer lineContainer = lineQuery.getContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXCONN", CONN);
    
    DBAction headQuery = database.table("EXT479").index("00").build();
    DBContainer headContainer = headQuery.getContainer();
    headContainer.set("EXCONO", CONO);
    headContainer.set("EXCONN", CONN);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      lineContainer.set("EXDLIX", record.DLIX as long);
      lineContainer.set("EXCONA", record.CONA);
      lineContainer.set("EXPAGE", record.PAGE as int);
      lineContainer.set("EXORNO", record.ORNO);
      lineContainer.set("EXPONR", record.PONR as int);
      lineContainer.set("EXITNO", record.ITNO);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      headContainer.set("EXDLIX", record.DLIX as long);
      headContainer.set("EXCONA", record.CONA);
      headContainer.set("EXPAGE", record.PAGE as int);
      
      headContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      headContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXCHNO", 0);
      headContainer.set("EXCHID", program.getUser());
      
      if(checkCONA(record.CONA)) {
        lineContainer.set("EXTYPE", 1);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 1);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
      } else {
        lineContainer.set("EXTYPE", 1);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 1);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
        
        lineContainer.set("EXTYPE", 2);
        lineContainer.set("EXAMNT", 0);
        headContainer.set("EXTYPE", 2);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
        
        lineContainer.set("EXTYPE", 3);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 3);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
      }
     
    }
  }
  
  /**
   * Insert data to the EXT480 table
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params LINES - containing the lines to be inserted
   * @return nothing
  */
  void insertDataToEXTD80(int CONO, long DLIX, ArrayList<Map<String, String>> LINES) {
    DBAction lineQuery = database.table("EXTD80").index("00").build();
    DBContainer lineContainer = lineQuery.getContainer();
    lineContainer.set("EXCONO", CONO);
    lineContainer.set("EXDLIX", DLIX);
    
    DBAction headQuery = database.table("EXTD79").index("00").build();
    DBContainer headContainer = headQuery.getContainer();
    headContainer.set("EXCONO", CONO);
    headContainer.set("EXDLIX", DLIX);
    
    for (int i=0; i<LINES.size(); i++) {
      Map<String, String> record = (Map<String, String>) LINES[i];
      
      lineContainer.set("EXCONA", record.CONA);
      lineContainer.set("EXPAGE", record.PAGE as int);
      lineContainer.set("EXORNO", record.ORNO);
      lineContainer.set("EXPONR", record.PONR as int);
      lineContainer.set("EXITNO", record.ITNO);
      
      lineContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      lineContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      lineContainer.set("EXCHNO", 0);
      lineContainer.set("EXCHID", program.getUser());
      
      headContainer.set("EXCONA", record.CONA);
      headContainer.set("EXPAGE", record.PAGE as int);
      
      headContainer.set("EXRGDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXRGTM", LocalTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger());
      headContainer.set("EXLMDT", LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger());
      headContainer.set("EXCHNO", 0);
      headContainer.set("EXCHID", program.getUser());
      
      if(checkCONA(record.CONA)) {
        lineContainer.set("EXTYPE", 1);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 1);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
      } else {
        lineContainer.set("EXTYPE", 1);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 1);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
        
        lineContainer.set("EXTYPE", 2);
        lineContainer.set("EXAMNT", 0);
        headContainer.set("EXTYPE", 2);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
        
        lineContainer.set("EXTYPE", 3);
        lineContainer.set("EXAMNT", 99.99);
        headContainer.set("EXTYPE", 3);
        
        lineQuery.insert(lineContainer);
        headQuery.insert(headContainer);
      }
     
    }
  }
  
  /**
   * Delete all records depending on CONN
   * @params CONO - Input company
   * @params CONN - Input shipment number
   * @params table - Input Xtend Table
   * @return nothing
  */
  void deleteRecordsByCONN(int CONO, int CONN, String table) {
    DBAction query = database.table(table).index("00").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXCONN", CONN);
    
    Closure<?> deleteRecords = {LockedResult result ->
      result.delete();
    }
    
    query.readAllLock(container, 2, deleteRecords);
  }
  
  /**
   * Delete all records depending on DLIX
   * @params CONO - Input company
   * @params DLIX - Input delivery number
   * @params table - Input Xtend Table
   * @return nothing
  */
  void deleteRecordsByDLIX(int CONO, long DLIX, String table) {
    DBAction query = database.table(table).index("00").build();
    DBContainer container = query.createContainer();
    container.set("EXCONO", CONO);
    container.set("EXDLIX", DLIX as long);
    
    Closure<?> deleteRecords = {LockedResult result ->
      result.delete();
    }
    
    query.readAllLock(container, 2, deleteRecords);
  }
  
}


















