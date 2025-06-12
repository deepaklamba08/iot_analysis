const BASE_URL = "http://127.0.0.1:5000"
const GET_ALL_JOBS_URL = BASE_URL+"/jobs/all"

function getAppDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/app/details'
}

function getJobDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/details'
}

function getJobHistoryUrl(jobName){
  return BASE_URL+"/jobs/history/"+jobName
}

function getJobRunUrl(){
  return BASE_URL+"/jobs/run/";
}

function getJobCurrentStatusUrl(jobName){
  return BASE_URL+"/jobs/status/"+jobName
}

function getJobSchedulerInfoUrl(){
  return BASE_URL+"/executor/info";
}

function getJobSchedulerUrl(){
  return BASE_URL+"/executor";
}

function getConfigUrl(configName){
  return BASE_URL+"/config/"+configName;
}

function failureCallback(error){
  console.error('API Call Error:', error);
}

function getCreateAppUrl(){
  return BASE_URL+"/app/create/";
}

function getAppNamesUrl(){
  return BASE_URL+"/app/names/";
}

function getApplicationDetailsUrl(appId){
  return BASE_URL+"/app/"+appId+"/details";
}

function getCreateJobUrl(){
  return BASE_URL+"/job/create/";
}


function makeAPICall(endPoint, methodType, requestBody, successCallback, failureCallback) {
    var options = {
        method: methodType,
        headers: {
            'Content-Type': 'application/json'
        }
    };

    if (requestBody && methodType.toUpperCase() !== 'GET') {
        options.body = JSON.stringify(requestBody);
    }

    fetch(endPoint, options)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (typeof successCallback === 'function') {
                successCallback(data);
            } else {
                console.warn("successCallback is not a function");
            }
        })
        .catch(error => {
            failureCallback(error);
        });
}

function populateJobNames(){
    makeAPICall(GET_ALL_JOBS_URL,'GET',null,function(responseData){
        const jobNamesSelect = document.getElementById('jobNames');
        jobNamesSelect.innerHTML = '';
        if (Array.isArray(responseData.data)) {
            responseData.data.forEach(job => {
                const option = document.createElement('option');
                option.value = job.name;
                option.textContent = job.name;
                jobNamesSelect.appendChild(option);
            });
           applicationDetails();
        } else {
            console.error("Invalid response format: 'data' is not an array.");
        }
    },failureCallback)
}

function clearTableContents(tableBody){
    var existingRows = tableBody.rows.length;
    if (existingRows > 0) {
        var existingColumns = tableBody.rows[0].cells.length
        for (var i = existingRows - 1; i >= 1; i--) {
            for (var j = existingColumns - 1; j >= 0; j--) {
                tableBody.rows[i].deleteCell(j);
            }
            tableBody.deleteRow(i);
        }
    }
}

function addRow(rawData,tableBody){
    var newRow = document.createElement('tr');
    var values = rawData.map(item => `<td>${item}</td>`).join('')
    var rowHtml = `<tr>${values}</tr>`;
    newRow.innerHTML=rowHtml;
    tableBody.appendChild(newRow);
}

function showSourceDetailsModal(sourceName) {
    document.getElementById("sourceDetailsModalLabel").textContent = sourceName;
    $('#sourceDetailsModal').modal('show');
}


function showTransformationDetailsModal(transformationName) {
    document.getElementById("transformationDetailsModalLabel").textContent = transformationName;
    $('#transformationDetailsModal').modal('show');
}

function showActionDetailsModal(actionName) {
    document.getElementById("actionDetailsModalLabel").textContent = actionName;
    $('#actionDetailsModal').modal('show');
}


function setApplicationDetails(responseData){
    var appDetails = responseData.data
    document.getElementById('appNameLabel').textContent = appDetails.name;
    document.getElementById('appDescriptionLabel').textContent = appDetails.description;
    document.getElementById('appStatusLabel').textContent = appDetails.status;
    document.getElementById('appCreateDateLabel').textContent = appDetails.create_date;
    document.getElementById('appLastModifiedLabel').textContent = appDetails.update_date;
    document.getElementById('appOwnerLabel').textContent = appDetails.created_by;
    document.getElementById('appConfigurationText').textContent = JSON.stringify(appDetails.config, null, 2);

    var appElementsTable = document.getElementById('applicationElementsTable')
    clearTableContents(appElementsTable);

    var sources = appDetails.sources;
    var transformations = appDetails.transformations;
    var actions = appDetails.actions;

    sources.forEach(element=>{
        var details = encodeURIComponent(JSON.stringify(element));
        var link=`<a href="#" class="source-link" style="color: Green;" data-details="${details}">${element.name}</a>`
        addRow([link,'Source',element.type,element.description],appElementsTable);
    });

    document.querySelectorAll('.source-link').forEach(link => {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            var details = this.dataset.details;
            var sourceDetails = JSON.parse(decodeURIComponent(details));
            showSourceDetailsModal('Source: '+sourceDetails.name)
        });
    });


    transformations.forEach(element=>{
        var details = encodeURIComponent(JSON.stringify(element));
        var link=`<a href="#" class="transformation-link" style="color: Blue;" data-details="${details}">${element.name}</a>`
        addRow([link,'Transformations',element.type,element.description],appElementsTable);
    });

    document.querySelectorAll('.transformation-link').forEach(link => {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            var details = this.dataset.details;
            var trDetails = JSON.parse(decodeURIComponent(details));
            showTransformationDetailsModal('Transformation: '+trDetails.name)
        });
    });

    actions.forEach(element=>{
        var details = encodeURIComponent(JSON.stringify(element));
        var link=`<a href="#" class="action-link" style="color: Orange;" data-details="${details}">${element.name}</a>`
        addRow([link,'Action',element.type,element.description],appElementsTable);
    });

    document.querySelectorAll('.action-link').forEach(link => {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            var details = this.dataset.details;
            var trDetails = JSON.parse(decodeURIComponent(details));
            showActionDetailsModal('Action: '+trDetails.name)
        });
    });
}

function initIndexPage(){
    populateJobNames();
}

function applicationDetails(){
    var jobName = document.getElementById('jobNames').value;
    var endPoint = getAppDetailsUrl(jobName)
    makeAPICall(endPoint,'GET',null,setApplicationDetails,failureCallback)
}

function openJobDetailsPage(){
    var jobName = document.getElementById('jobNames').value;
    window.open(`/job_details?name=${encodeURIComponent(jobName)}`, "_blank");
}

function getQueryParam(paramName){
    var params = new URLSearchParams(window.location.search);
    return params.get("name");
}

function closePage(){
    window.close();
}

function setJobDetails(responseData){
    if (responseData.status_code !== 200) {
        showMessageModal('Job details not available.');
        return;
    }

    var jobDetails = responseData.data;
    document.getElementById('jobNameLabel').textContent = jobDetails.name;
    document.getElementById('appIdLabel').textContent = jobDetails.application_id;
    document.getElementById('appNameLabel').textContent = jobDetails.application_name;
    document.getElementById('jobDescriptionLabel').textContent = jobDetails.description;
    document.getElementById('jobStatusLabel').textContent = jobDetails.status;
    document.getElementById('jobCreateDateLabel').textContent = jobDetails.create_date;
    document.getElementById('jobLastModifiedLabel').textContent = jobDetails.update_date;
    document.getElementById('jobOwnerLabel').textContent = jobDetails.created_by;
    document.getElementById('jobScheduledLabel').textContent = jobDetails.is_scheduled;

    var jobParameters = jobDetails.job_parameters;
    var jobParametersTextIp = document.getElementById("jobRunParametersInput");
    jobParametersTextIp.textContent = JSON.stringify(jobParameters, null, 2)
    jobParametersTextIp.disabled = true;

    var jobParametersTable = document.getElementById('jobParametersTable')
    clearTableContents(jobParametersTable);
    Object.keys(jobParameters).forEach(key => {
        addRow([key,'-',jobParameters[key]],jobParametersTable);
    });
}

function initJobDetailsPage(){
    var jobName=getQueryParam('name');
    if (name === null) {
        showMessageModal("Query parameter 'name' is not present.");
        return;
    }
    var endPoint = getJobDetailsUrl(jobName)
    makeAPICall(endPoint,'GET',null,setJobDetails,failureCallback)
}


function toggleParamsTable() {
    var isChecked = document.getElementById("provideParamsCheckbox").checked;
    var textarea = document.getElementById("jobRunParametersInput");
    textarea.disabled = !isChecked;
}

function readJobRunParams() {
    var params = document.getElementById("jobRunParametersInput").value;
    return params;
}


function runJob(){
    var jobName = document.getElementById('jobNameLabel').textContent.trim();
    if (jobName === '') {
        showMessageModal('Job details not available.');
        return;
    }

    var jobParameters = readJobRunParams();
    var url = getJobRunUrl()
    var requestData={
                    jobName: jobName,
                    jobParameters:jobParameters
                    };
    makeAPICall(url,'POST',requestData,function(response){
        $('#runJobModal').modal('hide');
        document.getElementById("messageModalBody").textContent = `Job '${jobName}' has been started successfully.`;
        $('#messageModal').modal('show');
    },failureCallback)
}

function showMessageModal(message,title='Info') {
    document.getElementById("messageModalLabel").textContent = title;
    document.getElementById("messageModalBody").textContent = message;
    $('#messageModal').modal('show');
}


function showMetricsMessageModal(title='Metrics') {
    document.getElementById("metricsMessageModalLabel").textContent = title;
    $('#metricsMessageModal').modal('show');
}

function openJobHistoryPage(){
    var jobName = document.getElementById('jobNames').value;
    window.open(`/job_history?name=${encodeURIComponent(jobName)}`, "_blank");
}

function openSchedulerPage(){
    window.open(`/scheduler`, "_blank");
}

function openCreateAppPage(){
    window.open(`/create_application`, "_blank");
}

function openCreateJobPage(){
    window.open(`/create_job`, "_blank");
}

function setJobCurrentStatus(responseData){
    if (responseData.status_code !== 200) {
        showMessageModal('No history available for this job. Please run the job to see the history.');
        return;
    }
    var jobStatus = responseData.data;
    var jobName=getQueryParam('name');

    document.getElementById('jobNameLabel').textContent = jobName;
    document.getElementById('appIdLabel').textContent = jobStatus.app_id;
    document.getElementById('appNameLabel').textContent = jobStatus.app_name;
    document.getElementById('jobRunByLabel').textContent = jobStatus.run_by;
    document.getElementById('jobRunTypeLabel').textContent = jobStatus.run_type;
    document.getElementById('jobStatusLabel').textContent = jobStatus.status;
    document.getElementById('jobStartDateLabel').textContent = jobStatus.start_time;
    document.getElementById('jobEndDateLabel').textContent = jobStatus.end_time;
    document.getElementById('jobMessageLabel').textContent = jobStatus.message;
}

function setJobHistory(responseData){
    if (responseData.status_code !== 200) {
        showMessageModal('No history available for this job. Please run the job to see the history.');
        return;
    }
    var jobHistory = responseData.data;
    var jobExeDetailsTable = document.getElementById('jobExeDetailsTable')
    clearTableContents(jobExeDetailsTable);

     jobHistory.forEach(element=>{
        var metrics = encodeURIComponent(JSON.stringify(element.metrics));
        var color = element.status === 'Failed' ? 'red' : 'Green';
        var link=`<a href="#" class="status-link" style="color: ${color};" data-metrics="${metrics}">${element.status}</a>`
        addRow([element.run_by,element.run_type,link,element.start_time,element.end_time,element.message],jobExeDetailsTable);
    });

    document.querySelectorAll('.status-link').forEach(link => {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            var metrics = this.dataset.metrics;
            var metricsData = JSON.parse(decodeURIComponent(metrics));
            if (metricsData.hasOwnProperty('databag_metrics')) {
                var metricsTable = document.getElementById('metricsMessageModalTable');
                clearTableContents(metricsTable)
                metricsData.databag_metrics.forEach(element=>{
                   addRow([element.type,element.name,element.provider,element.records],metricsTable);
                });
                showMetricsMessageModal();
            } else {
                showMessageModal('Metrics not available.','Metrics');
            }

        });
    });

}


function populateJobCurrentStatus(jobName){
    var statusEndPoint = getJobCurrentStatusUrl(jobName);
    makeAPICall(statusEndPoint,'GET',null,setJobCurrentStatus,failureCallback);
}

function populateJobHistory(jobName){
    var historyEndPoint=getJobHistoryUrl(jobName);
    makeAPICall(historyEndPoint,'GET',null,setJobHistory,failureCallback);
}

function initHistoryPage(){
    var jobName = getQueryParam('name');
    if (name === null) {
        showMessageModal("Query parameter 'name' is not present.");
        return;
    }
    populateJobCurrentStatus(jobName);
    populateJobHistory(jobName);
}


function setSchedulerInfo(responseData){
    if (responseData.status_code !== 200) {
        showMessageModal('No Scheduler information available.');
        return;
    }
    var info = responseData.data;
    document.getElementById('schedulerNameLabel').textContent = info.name;
    document.getElementById('schedulerDescriptionLabel').textContent = info.description;
    document.getElementById('schedulerStatusLabel').textContent = info.status;
    document.getElementById('schedulerState').textContent = info.current_state;
    document.getElementById('schedulerCreateDateLabel').textContent = info.create_date;
    document.getElementById('schedulerOwnerLabel').textContent = info.created_by;

    if(info.config.hasOwnProperty('params')){
       var schParamsTable = document.getElementById('schedulerConfigurationTable')
       clearTableContents(schParamsTable);
       var parameters = info.config.params;
       parameters.forEach(schParam => {
         addRow([schParam.name,schParam.description,schParam.value],schParamsTable);
       }
       )
    }

    toggleSchButton(info.current_state);
}

function toggleSchButton(current_state){

    if(current_state === 'running') {
        document.getElementById('startSchedulerButton').disabled = true;
        document.getElementById('stopSchedulerButton').disabled = false;
    } else if(current_state === 'stopped') {
        document.getElementById('startSchedulerButton').disabled = false;
        document.getElementById('stopSchedulerButton').disabled = true;
    }else{
        showMessageModal("Scheduler is in invalid state.");
    }
}

function initSchedulerPage(){
    var url = getJobSchedulerInfoUrl()
    makeAPICall(url,'GET',null,setSchedulerInfo,failureCallback)
}

function schedulerAction(action,message){
    var requestData = {
        "action": action
    };
    var url = getJobSchedulerUrl()
    makeAPICall(url,'PUT',requestData,function(response){
        if(response.status_code === 200) {
            var schAction = action === 'start' ? 'running' : 'stopped';
            document.getElementById('schedulerState').textContent = schAction;
            toggleSchButton(schAction);
            showMessageModal(message);
        } else {
            showMessageModal("Something went wrong. Please try again later.");
        }
    },failureCallback)
}

function startScheduler(){
    schedulerAction('start','Scheduler started successfully.');
}

function stopScheduler(){
    schedulerAction('stop','Scheduler stopped successfully.');
}

function createApplication(){
    var appName = document.getElementById('appNameField').value;
    var appDesc = document.getElementById('appDescriptionField').value;
    var appConfig = document.getElementById('appConfig').value;

    var table = document.getElementById("createAppElementsTable");
    var rows = table.getElementsByTagName("tr");
    var appObj = {
        name: appName,
        description: appDesc,
        status: true,
        config: JSON.parse(appConfig),
    }
    var sources=[]
    var transformations=[]
    var actions=[]

    for (let i = 1; i < rows.length; i++) { // Skip header row
      var cells = rows[i].getElementsByTagName("td");
      var name = cells[0].innerText;
      var type = cells[1].innerText;
      var description = cells[2].innerText;

      var object = {
          name: name,
          description: description,
          status:true,
          type: type.split('-')[1].trim(),
          config:{}
      }

      if(type.startsWith('Source')) {
        sources.push(object);
      }else if(type.startsWith('Transformation')) {
        transformations.push(object);
      }else if(type.startsWith('Action')) {
        actions.push(object);
      }
    }
    if(sources.length== 0 || actions.length == 0) {
       showMessageModal('Source and action must be provided.');
       return;
    }

    if(sources.length != 0) {
        appObj.sources = sources;
    }
    if(transformations.length != 0) {
        appObj.transformations = transformations;
    }
    if(actions.length != 0) {
        appObj.actions = actions;
    }

    var url = getCreateAppUrl();
    makeAPICall(url,'POST',appObj,function(response){
        if(response.status_code === 200) {
            document.getElementById("messageModalBody").textContent = `Application created.`;
        }else{
            document.getElementById("messageModalBody").textContent = `Error occurred while creating application.`;
        }
        $('#messageModal').modal('show');
    },failureCallback)

    showMessageModal('Application Created!!');
}

function discardCreateApplication(){


    showMessageModal('Application Discarded!!');
}

function addSource(){
    var srcName = document.getElementById('sourceNameField').value;
    var srcDesc = document.getElementById('sourceDescriptionField').value;
    var srcConfig = document.getElementById('sourceConfig').value;
    var srcType = document.getElementById('sourceType').value;

    var appElementsTable = document.getElementById('createAppElementsTable');
    addRow([srcName,"Source- "+srcType,srcDesc],appElementsTable);

    showMessageModal('Source Added!!');

    document.getElementById('sourceNameField').value='';
    document.getElementById('sourceDescriptionField').value='';
    document.getElementById('sourceConfig').value='';

    $('#addSourceModal').modal('hide');
}

function addTransformation(){
    var trName = document.getElementById('transformationNameField').value;
    var trDesc = document.getElementById('transformationDescriptionField').value;
    var trConfig = document.getElementById('transformationConfig').value;
    var trType = document.getElementById('transformationType').value;

    var appElementsTable = document.getElementById('createAppElementsTable');
    addRow([trName,"Transformation- "+trType,trDesc],appElementsTable);

    showMessageModal('Transformation Added!!');

    document.getElementById('transformationNameField').value='';
    document.getElementById('transformationDescriptionField').value='';
    document.getElementById('transformationConfig').value='';

    $('#addTransformationModal').modal('hide');
}

function addAction(){
    var actName = document.getElementById('actionNameField').value;
    var actDesc = document.getElementById('actionDescriptionField').value;
    var actConfig = document.getElementById('actionConfig').value;
    var actType = document.getElementById('actionType').value;

    var appElementsTable = document.getElementById('createAppElementsTable');
    addRow([actName,"Action- "+actType,actDesc],appElementsTable);

    showMessageModal('Action Added!!');

    document.getElementById('actionNameField').value='';
    document.getElementById('actionDescriptionField').value='';
    document.getElementById('actionConfig').value='';

    $('#addActionModal').modal('hide');
}

function initCreateAppPage(){
    document.getElementById('appNameField').value= '';
    document.getElementById('appDescriptionField').value= '';
    document.getElementById('appConfig').value= '';
    var url = getConfigUrl("element_config");
    //set app elements
    makeAPICall(url,'GET',null,setAppElements);
}

function setAppElements(responseData){
    if (responseData.status_code !== 200) {
        console.error('Failed to fetch application elements:', responseData.message);
        return;
    }
    setElementValues('sourceType',responseData.data.sources);
    setElementValues('transformationType',responseData.data.transformations);
    setElementValues('actionType',responseData.data.actions);

}

function setElementValues(element,values){
    var elementTypeSelect = document.getElementById(element);
    elementTypeSelect.innerHTML = '';
    if (Array.isArray(values)) {
        values.forEach(value => {
            var option = document.createElement('option');
            option.value = value.name;
            option.textContent = value.name;
            elementTypeSelect.appendChild(option);
        });
    } else {
        console.error("Invalid response format: 'data' is not an array.");
    }
}

function initCreateJobPage(){
    var url = getAppNamesUrl()
    var appNamesSelect = document.getElementById('applicationSelect');
    makeAPICall(url,'GET',null,function(responseData){
        if(responseData.status_code !== 200) {
            showMessageModal('No applications available to create job.');
            return;
        }
        appNamesSelect.innerHTML = '';
        Object.entries(responseData.data).forEach(([appName, appId]) => {
            var option = document.createElement('option');
            option.value = appId;
            option.textContent = appName;
            appNamesSelect.appendChild(option);
        });
    },failureCallback);
    appNamesSelect.addEventListener('change', function() {
        handleApplicationSelectChange(this.value);
    });
    var appElementsTable = document.getElementById('appElementsTable')
    clearTableContents(appElementsTable);
}
function handleApplicationSelectChange(selectedValue) {
    makeAPICall(getApplicationDetailsUrl(selectedValue),'GET',null,function(responseData){
        if(responseData.status_code !== 200) {
            showMessageModal('Application details not available.');
            return;
        }
        var appDetails = responseData.data;
        document.getElementById('jobConfig').textContent = JSON.stringify(appDetails.config, null, 2);

        var appElementsTable = document.getElementById('appElementsTable')
        clearTableContents(appElementsTable);
        appDetails.sources.forEach(source => {
            addRow([source.name, source.type, source.description], appElementsTable);
        });
    });
}

function createJob(){
    var jobName = document.getElementById('jobNameField').value;
    var jobDesc = document.getElementById('jobDescriptionField').value;
    var appId = document.getElementById('applicationSelect').value;
    var jobConfig = document.getElementById('jobConfig').value;

    if (jobName === '' || appId === '') {
        showMessageModal('Job name and application must be provided.');
        return;
    }

    var requestData = {
        name: jobName,
        description: jobDesc,
        application_id: appId,
        application_name: document.getElementById('applicationSelect').selectedOptions[0].textContent,
        status:true,
        config: JSON.parse(jobConfig)
    };

    makeAPICall(getCreateJobUrl(), 'POST', requestData, function(response) {
        if (response.status_code === 200) {
            showMessageModal(`Job '${jobName}' created successfully.`);
        } else {
            showMessageModal(`Error occurred while creating job.`);
        }
    }, failureCallback);
}
