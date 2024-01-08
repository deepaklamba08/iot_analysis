const BASE_URL = "http://127.0.0.1:5000"
const GET_ALL_JOBS_URL = BASE_URL+"/jobs/all"

const DATA_TABLE_NAME="dataTable"
const DATA_TABLE_BODY_NAME="dataTableBody"
const APP_DETAILS_LABEL_NAME="appDetailsLabel"

const DATA_TABLE_PANEL_ID="table_data_panel"
const RUN_JOB_PARAM_PANEL_ID="appRunLabelPanel"

const SOURCE_TABLE_ID="sources_table"
const SOURCE_TABLE_BODY_ID="sources_table_body"
const SOURCE_TABLE_PARA_ID="sources_table_para"
const SOURCE_TABLE_PARA_TEXT="Sources:"

const TRANSFORMATION_TABLE_ID="transformations_table"
const TRANSFORMATION_TABLE_BODY_ID="transformations_table_body"
const TRANSFORMATION_TABLE_PARA_ID="transformations_table_para"
const TRANSFORMATION_TABLE_PARA_TEXT="Transformations:"

const ACTION_TABLE_ID="actions_table"
const ACTION_TABLE_BODY_ID="actions_table_body"
const ACTION_TABLE_PARA_ID="actions_table_para"
const ACTION_TABLE_PARA_TEXT="Actions:"

const RUN_METRICS_TABLE_ID="run_metrics_table"
const RUN_METRICS_TABLE_BODY_ID="run_metrics_table_body"
const RUN_METRICS_TABLE_PARA_ID="run_metrics_table_para"
const RUN_METRICS_TABLE_PARA_TEXT="Run Metrics:"

const JOB_PARAMETERS_TABLE_ID="job_parameters_table"
const JOB_PARAMETERS_TABLE_BODY_ID="job_parameters_table_body"
const JOB_PARAMETERS_TABLE_PARA_ID="job_parameters_table_para"
const JOB_PARAMETERS_TABLE_PARA_TEXT="Job Parameters:"

const RUN_JOB_PARAMETERS_TABLE_ID="jobParamTable"
const RUN_JOB_PARAMETERS_TABLE_BODY_ID="jobParamTableBody"


const DYNAMIC_ELEMENT_IDS=[SOURCE_TABLE_ID,SOURCE_TABLE_BODY_ID,SOURCE_TABLE_PARA_ID,
TRANSFORMATION_TABLE_ID,TRANSFORMATION_TABLE_BODY_ID,TRANSFORMATION_TABLE_PARA_ID,
ACTION_TABLE_ID,ACTION_TABLE_BODY_ID,ACTION_TABLE_PARA_ID,
JOB_PARAMETERS_TABLE_ID,JOB_PARAMETERS_TABLE_BODY_ID,JOB_PARAMETERS_TABLE_PARA_ID,
RUN_METRICS_TABLE_ID,RUN_METRICS_TABLE_BODY_ID,RUN_METRICS_TABLE_PARA_ID]


function getJobRunUrl(jobName){
  return BASE_URL+"/jobs/run/"; //+jobName
}

function getJobCurrentStatusUrl(jobName){
  return BASE_URL+"/jobs/status/"+jobName
}
function getJobHistoryUrl(jobName){
  return BASE_URL+"/jobs/history/"+jobName
}

function getAppDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/app/details'
}

function getJobDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/details'
}

function addJobs() {
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS);
    clearTable(RUN_JOB_PARAMETERS_TABLE_ID);

    var jobNamesDropDown = document.getElementById("jobNames");
    for (let i = jobNames.length-1; i >=0 ; i--) {
       jobNamesDropDown.remove(i);
    }

    const options = {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    };

    fetch(GET_ALL_JOBS_URL,options)
    .then(response => response.json())
    .then(response_object => {
        if(response_object.status_code!=200){
            alert(response_object.message)
        }
        else
        {
        var response_data = response_object.data
        var data = new Array(response_data.length);
        for (let i = 0 ; i <=response_data.length-1 ; i++) {
        var option = document.createElement("option");
            option.text = response_data[i].name;
            jobNamesDropDown.add(option);
            data[i] = [response_data[i].object_id,response_data[i].name,response_data[i].status,response_data[i].description];
        }
        populateDataInTable(DATA_TABLE_NAME,DATA_TABLE_BODY_NAME,["Job Id","Name","Status","Description"],data,false)
        setLabel(APP_DETAILS_LABEL_NAME,"All Jobs")
        }
    }).catch(error => {
      if (error instanceof TypeError && error.message.includes('API key')) {
        console.error('Invalid API key:', error);
      } else {
        console.error('There was a problem with the Fetch operation:', error);
      }
    });

    setLabel(APP_DETAILS_LABEL_NAME,"All Jobs")
}

function getSelectedJobName(){

    var jobNamesDropDown = document.getElementById('jobNames');
    var selectedIndex = jobNamesDropDown.selectedIndex;
    var selectedValue = jobNamesDropDown.options[selectedIndex].text;
    return selectedValue;
}

function runJob(){
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS);

    var jobName=getSelectedJobName()
    var url=getJobRunUrl(jobName)
    var jobParameters=readJobParametersHTMLTable(RUN_JOB_PARAMETERS_TABLE_ID);
    var requestData={jobParameters:JSON.stringify(Object.fromEntries(jobParameters)),jobName:jobName};

    const options = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(requestData)
    };

    fetch(url, options)
      .then(response => {
        if (!response.ok) {
          console.log(`HTTP error ${response.status}`);
        }
        alert("Job '"+jobName+"' has been triggered")
      }).catch(error => {
        console.error('Error updating data:', error);
      });
    fetchCurrentStatus(jobName)
}

function getJobHistory(jobName){
    var url=getJobHistoryUrl(jobName)
    const options = {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    };

    fetch(url, options)
      .then(response =>{
        if (!response.ok) {
          console.log(`HTTP error ${response.status}`);
        }
       return response.json();
      }).then(response_object => {
        if(response_object.status_code!=200){
            alert(response_object.message)
        }
        else
        {
        var response_data=response_object.data;
        var data = new Array(response_data.length);
        for (let i = 0 ; i <=response_data.length-1 ; i++) {
            data[i] = [/*response_data[i].job_id,*/response_data[i].run_by,response_data[i].status,response_data[i].start_time,response_data[i].end_time,response_data[i].message];
        }
        populateDataInTable(DATA_TABLE_NAME,DATA_TABLE_BODY_NAME,[/*"Job Id",*/"Submitter","Status","Start Time","End Time","Message"],data,false)
        setLabel(APP_DETAILS_LABEL_NAME,"Job History: "+jobName)
        }
      }).catch(error => {
        console.error('Error updating data:', error);
      });

}

function jobHistory(){
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS)
    var jobName=getSelectedJobName()
    getJobHistory(jobName)
}

function applicationDetails(){
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS)
    var jobName=getSelectedJobName()
    var url=getAppDetailsUrl(jobName)
    const options = {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    };

    fetch(url, options)
      .then(response => {
        if (!response.ok) {
          console.log(`HTTP error ${response.status}`);
        }
        return response.json() ;
      }).then(response_object => {
          if(response_object.status_code!=200){
            alert(response_object.message)
           }else{
          var response_data=response_object.data;
          var data = [["Id",response_data.object_id],
          ["Name",response_data.name],
          ["Status",response_data.status],
          ["Created By",response_data.created_by],
          ["Updated By",response_data.updated_by],
          ["Create Date",response_data.create_date],
          ["Update Date",response_data.update_date],
          ["Description",response_data.description],
          ["Sources",response_data.source_count],
          ["Transformations",response_data.transformation_count],
          ["Actions",response_data.action_count]];
          populateDataInTable(DATA_TABLE_NAME,DATA_TABLE_BODY_NAME,["Property","Value"],data,false)

          var sourcesDataTable = new Array(response_data.sources.length);
          for (let i = 0 ; i <=sourcesDataTable.length-1 ; i++) {
           var sourceData = response_data.sources[i]
           sourcesDataTable[i] = [sourceData.name,sourceData.status,sourceData.type,sourceData.description];
          }
          createHTMLTable(DATA_TABLE_PANEL_ID,SOURCE_TABLE_ID,SOURCE_TABLE_BODY_ID,SOURCE_TABLE_PARA_ID,SOURCE_TABLE_PARA_TEXT,
            ["Name","Status","Type","Description"],sourcesDataTable)


          var transformationsDataTable = new Array(response_data.transformations.length);
          for (let i = 0 ; i <=transformationsDataTable.length-1 ; i++) {
           var transformationData = response_data.transformations[i]
           transformationsDataTable[i] = [transformationData.name,transformationData.status,transformationData.type,transformationData.description];
          }
          createHTMLTable(DATA_TABLE_PANEL_ID,TRANSFORMATION_TABLE_ID,TRANSFORMATION_TABLE_BODY_ID,TRANSFORMATION_TABLE_PARA_ID,TRANSFORMATION_TABLE_PARA_TEXT,
            ["Name","Status","Type","Description"],transformationsDataTable)


          var actionsDataTable = new Array(response_data.actions.length);
          for (let i = 0 ; i <=actionsDataTable.length-1 ; i++) {
           var actionData = response_data.actions[i]
           actionsDataTable[i] = [actionData.name,actionData.status,actionData.type,actionData.description];
          }
          createHTMLTable(DATA_TABLE_PANEL_ID,ACTION_TABLE_ID,ACTION_TABLE_BODY_ID,ACTION_TABLE_PARA_ID,ACTION_TABLE_PARA_TEXT,
            ["Name","Status","Type","Description"],actionsDataTable)
          setLabel(APP_DETAILS_LABEL_NAME,"Application Details: "+response_data.name)

           }
      }).catch(error => {
        console.error('Error updating data:', error);
      });
}

function jobDetails(){
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS);
    clearTable(RUN_JOB_PARAMETERS_TABLE_ID);
    var jobName=getSelectedJobName()
    var url=getJobDetailsUrl(jobName)
    const options = {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      }
    };

    fetch(url, options)
      .then(response => {
        if (!response.ok) {
          console.log(`HTTP error ${response.status}`);
        }
        return response.json() ;
      }).then(response_object => {
            if(response_object.status_code!=200){
            alert(response_object.message)
           }else{
           var response_data=response_object.data;
          var data = [["Id",response_data.object_id],
          ["Name",response_data.name],
          ["Status",response_data.status],
          ["Created By",response_data.created_by],
          ["Updated By",response_data.updated_by],
          ["Create Date",response_data.create_date],
          ["Update Date",response_data.update_date],
          ["Description",response_data.description],
          ["Application Id",response_data.application_id],
          ["Is Scheduled",response_data.is_scheduled]];
          populateDataInTable(DATA_TABLE_NAME,DATA_TABLE_BODY_NAME,["Property","Value"],data,false)

          var jobParametersTable = new Array();
          var jobParameters = response_data.job_parameters;
          createJobRunInputElements(jobName,jobParameters);
            var i=0;
            for (const key in jobParameters) {
              if (jobParameters.hasOwnProperty(key)) {
                jobParametersTable[i] = [key,jobParameters[key]]
                i++;
              }
            }
            if(jobParametersTable.length>0){
            createHTMLTable(DATA_TABLE_PANEL_ID,JOB_PARAMETERS_TABLE_ID,JOB_PARAMETERS_TABLE_BODY_ID,JOB_PARAMETERS_TABLE_PARA_ID,JOB_PARAMETERS_TABLE_PARA_TEXT,
            ["Parameter Name","Value"],jobParametersTable)
            }
             setLabel(APP_DETAILS_LABEL_NAME,"Job Details: "+jobName)
          }

      }).catch(error => {
        console.error('Error updating data:', error);
      });

}

function fetchCurrentStatus(jobName){
        var url=getJobCurrentStatusUrl(jobName)
        const options = {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
        };

    fetch(url, options)
      .then(response =>{
        if (!response.ok) {
          console.log(`HTTP error ${response.status}`);
        }
       return response.json();
      }).then(response_object => {
       if(response_object.status_code!=200){
            alert(response_object.message)
           }else{
                  var response_data=response_object.data;
       var data = [
          ["Job Id",response_data.job_id],
          ["App Id",response_data.app_id],
          ["Run By",response_data.run_by],
          ["Run Type",response_data.run_type],
          ["Status",response_data.status],
          ["Start Time",response_data.start_time],
          ["End Time",response_data.end_time],
          ["Message",response_data.message]];
          populateDataInTable(DATA_TABLE_NAME,DATA_TABLE_BODY_NAME,["Property","Value"],data,false)

          if (response_data.metrics != null){
          var metricsTableData = new Array();


          for (let i = 0 ; i <response_data.metrics.databag_metrics.length ; i++) {
           var databagMetric = response_data.metrics.databag_metrics[i]
           metricsTableData[i] = [databagMetric.type,databagMetric.name,databagMetric.provider,databagMetric.records];
          }

          createHTMLTable(DATA_TABLE_PANEL_ID,RUN_METRICS_TABLE_ID,RUN_METRICS_TABLE_BODY_ID,RUN_METRICS_TABLE_PARA_ID,RUN_METRICS_TABLE_PARA_TEXT,
            ["Type","Name","Provider","Records"],metricsTableData)

          }

          setLabel(APP_DETAILS_LABEL_NAME,"Current status: "+jobName)
           }
      }).catch(error => {
        console.error('Error updating data:', error);
      });
}

function currentStatus(){
    removeElementsFromDocument(DYNAMIC_ELEMENT_IDS)
    var jobName=getSelectedJobName()
    fetchCurrentStatus(jobName)
}

function setLabel(elementId,labelText){
    var appDetailsLabel = document.getElementById(elementId);
    appDetailsLabel.innerHTML = labelText
}

function clearTable(tableName){
var tbl = document.getElementById(tableName);
  var existingRows = tbl.rows.length;
  if(existingRows >0){
    var existingColumns = tbl.rows[0].cells.length
     for (var i = existingRows-1; i >=0; i--) {
        for (var j = existingColumns-1; j >=0; j--) {
        tbl.rows[i].deleteCell(j);
        }
        tbl.deleteRow(i);
    }
    }
}

function populateDataInTable(tableName,tableBodyName,columnHeaders,data,editable) {
  var tbl = document.getElementById(tableName);
  clearTable(tableName);
  var tblBody = document.getElementById(tableBodyName);

  var  rows=data.length;
  var  columns=columnHeaders.length;

  var headerRow = document.createElement("tr");
  for (var i = 0; i < columns; i++) {
        var th = document.createElement("th");
        var input = document.createElement("input");
        input.setAttribute("type", "text");
        input.setAttribute("class", "header-input");
        input.setAttribute("placeholder", columnHeaders[i]);
        th.appendChild(input);
        headerRow.appendChild(th);
      }
      tbl.appendChild(headerRow);

  for (let i = 0; i < rows; i++) {
    const row = document.createElement("tr");
    var dataRow=data[i];
    for (let j = 0; j <columns; j++) {
      const cell = document.createElement("td");
      const cellText = document.createTextNode(dataRow[j]);
      cell.appendChild(cellText);
      if(editable){
      cell.setAttribute("contenteditable","true");
      }
      row.appendChild(cell);
    }
    tblBody.appendChild(row);
  }

  tbl.appendChild(tblBody);
  tbl.setAttribute("border", "3");
}

function removeElementsFromDocument(elementIds){
 for (let i = 0; i < elementIds.length; i++) {
    var element = document.getElementById(elementIds[i])
    if (element != null){
        element.remove();
    }
 }

}

function createJobRunInputElements(jobName,jobParameters){
    var sourcePanel = document.getElementById(RUN_JOB_PARAM_PANEL_ID);
    var paramsArray = new Array(jobParameters.length);

    var i=0;
            for (const key in jobParameters) {
              if (jobParameters.hasOwnProperty(key)) {
              paramsArray[i] = [key,jobParameters[key]];
              i++;
              }
              }

    if(i>0){
    setLabel("jobParametersLabel","Edit Available Parameters For Job: "+jobName)
    populateDataInTable(RUN_JOB_PARAMETERS_TABLE_ID,RUN_JOB_PARAMETERS_TABLE_BODY_ID,["Parameter","Value"],paramsArray,true);
    }else{
    setLabel("jobParametersLabel","No Parameters Available")
    }

}

function createHTMLTable(sourcePanelId,tableId,tableBodyId,tableDescParaId,tableDescription,columnHeaders,data) {
    removeElementsFromDocument([tableId,tableBodyId,tableDescParaId])
    var sourcePanel = document.getElementById(sourcePanelId);

    var table = document.createElement('TABLE');
    table.border='3';
    table.id=tableId

    var tableBody = document.createElement('TBODY');
    tableBody.id=tableBodyId
    table.appendChild(tableBody);
    var tableDescPara = document.createElement('p');
    tableDescPara.id=tableDescParaId
    tableDescPara.innerHTML = tableDescription;
    sourcePanel.appendChild(tableDescPara)
    sourcePanel.appendChild(table);

    populateDataInTable(tableId,tableBodyId,columnHeaders,data,false)

}

function readJobParametersHTMLTable(tableId){
    var oTable = document.getElementById(tableId);
    var rowLength = oTable.rows.length;
    var tableData = new Map();

    for (i = 1; i < rowLength; i++){
        var oCells = oTable.rows.item(i).cells;
        var cellLength = oCells.length;

        var key;
        var value;
       for(var j = 0; j < cellLength; j++){
            var cellVal = oCells.item(j).innerHTML;
            if(j==0){
            key=oCells.item(j).innerHTML;
            }
            if(j==1){
            value=oCells.item(j).innerHTML;
            }
            tableData.set(key,value);
   }
}
    return tableData;
}
