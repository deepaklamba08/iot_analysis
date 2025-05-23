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

function failureCallback(error){
  console.error('API Call Error:', error);
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

    var jobParametersTable = document.getElementById('jobParametersTable')
    clearTableContents(jobParametersTable);

    var jobRunParametersTable = document.getElementById('jobRunParametersTableBody')
    clearTableContents(jobRunParametersTable);

    var jobParameters = jobDetails.job_parameters;
    Object.keys(jobParameters).forEach(paramName => {
        addRow([paramName,'-',jobParameters[paramName]],jobParametersTable);
        addRow([paramName,jobParameters[paramName]],jobRunParametersTable);
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
    var inputs = document.querySelectorAll("#jobRunParametersTableBody input");
    inputs.forEach(input => {
        input.disabled = !isChecked;
    });
}

function readTableParameters() {
    var table = document.getElementById("jobRunParametersTableBody");
    var parameters = {};
    for (let i = 0; i < table.rows.length; i++) {
        var cells = table.rows[i].cells;
        var key = cells[0].textContent.trim();
        var value = cells[1].textContent.trim();
        if (key) {
            parameters[key] = value;
        }
    }
    return parameters;
}


function runJob(){
    var jobName = document.getElementById('jobNameLabel').textContent.trim();
    if (jobName === '') {
        showMessageModal('Job details not available.');
        return;
    }

    var jobParameters = readTableParameters();
    var url = getJobRunUrl()
    var requestData={
                    jobName: jobName,
                    jobParameters:JSON.stringify(jobParameters)
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
                console.log(JSON.stringify(metricsData, null, 2))
                var metricsTable = document.getElementById('metricsMessageModalTable');
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
    document.getElementById('schedulerCreateDateLabel').textContent = info.create_date;
    document.getElementById('schedulerOwnerLabel').textContent = info.created_by;

}

function initSchedulerPage(){
    var url = getJobSchedulerInfoUrl()
    makeAPICall(url,'GET',null,setSchedulerInfo,failureCallback)
}

function startScheduler(){
    var requestData = {
        "action": "start"
    };
    var url = getJobSchedulerUrl()
    makeAPICall(url,'PUT',requestData,function(response){
        showMessageModal('Scheduler started successfully.');
    },failureCallback)
}