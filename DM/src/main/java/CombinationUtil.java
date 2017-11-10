import java.util.ArrayList;
import java.util.List;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class CombinationUtil {
	private List<String> combinationStrArr;
	
	public CombinationUtil(){
		this.combinationStrArr = new ArrayList<String>();
	}
	
	//getCombination, parameter: String[] dataList
	public List<String> getCombination(String[] dataList, int n){
		for(int j = n; j <= dataList.length; j++ ){
			genCombination(dataList,0,new String[j],0);
		}
		return this.combinationStrArr;
	}
	//getCombination(Overloading), parameter: List<String> dataList
	public List<String> getCombination(List<String> dataList, int n){
		for(int j = n; j <= dataList.size(); j++ ){
			genCombination(dataList,0,new String[j],0);
		}
		return this.combinationStrArr;
	}
	
	//genCombination, parameter: String[] dataList
	private void genCombination(String[] dataList, int dataIndex, String[] resultList, int resultIndex){
		int resultLen = resultList.length;  
        int resultCount = resultIndex + 1;  
        if (resultCount > resultLen) { //end iterator 
        	String resultStr = "";
        	for(int i = 0; i < resultList.length; i++){
        		if(i == resultList.length - 1){
        			resultStr += resultList[i];
        		} else {
        			resultStr += resultList[i] + RetailAnalysis.SEPARATOR;
        		}
        	}
        	combinationStrArr.add(resultStr);
        	return;
        }  
  
        //iterator 
        for (int i = dataIndex; i < dataList.length + resultCount - resultLen; i++) {  
            resultList[resultIndex] = dataList[i];  
            genCombination(dataList, i + 1, resultList, resultIndex + 1);  
        }
	}
	//genCombination(Overloading), parameter: List<String> dataList
	private void genCombination(List<String> dataList, int dataIndex, String[] resultList, int resultIndex){
		int resultLen = resultList.length;  
        int resultCount = resultIndex + 1;  
        if (resultCount > resultLen) { //end iterator 
        	String resultStr = "";
        	for(int i = 0; i < resultList.length; i++){
        		if(i == resultList.length - 1){
        			resultStr += resultList[i];
        		} else {
        			resultStr += resultList[i] + RetailAnalysis.SEPARATOR;
        		}
        	}
        	combinationStrArr.add(resultStr);
        	return;
        }  
  
        //iterator 
        for (int i = dataIndex; i < dataList.size() + resultCount - resultLen; i++) {  
            resultList[resultIndex] = dataList.get(i);  
            genCombination(dataList, i + 1, resultList, resultIndex + 1);  
        }
	}
}
