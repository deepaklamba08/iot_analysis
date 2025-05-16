const BASE_URL = "http://127.0.0.1:5000"
const GET_ALL_JOBS_URL = BASE_URL+"/jobs/all"

function getAppDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/app/details'
}

function getJobDetailsUrl(jobName){
  return BASE_URL+"/jobs/"+jobName+'/details'
}

function makeAPICall(endPoint, methodType, requestBody, consumer) {
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
            if (typeof consumer === 'function') {
                consumer(data);
            } else {
                console.warn("Consumer is not a function");
            }
        })
        .catch(error => {
            console.error('API Call Error:', error);
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
        } else {
            console.error("Invalid response format: 'data' is not an array.");
        }
    })
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

function addRaw(rawData,tableBody){
    var newRow = document.createElement('tr');
    var values = rawData.map(item => `<td>${item}</td>`).join('')
    var rowHtml = `<tr>${values}</tr>`;
    newRow.innerHTML=rowHtml;
    tableBody.appendChild(newRow);
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
        var value = "source_detail.html?id="+encodeURIComponent(element.object_id);
        var link = `<a href=${value} target="_blank">${element.name}</a>`;
        addRaw([link,'Source',element.type,element.description],appElementsTable);
    });
    transformations.forEach(element=>{
        var value = "source_detail.html?id="+encodeURIComponent(element.object_id);
        var link = `<a href=${value} target="_blank">${element.name}</a>`;
        addRaw([link,'Transformations',element.type,element.description],appElementsTable);
    });
    actions.forEach(element=>{
        var value = "source_detail.html?id="+encodeURIComponent(element.object_id);
        var link = `<a href=${value} target="_blank">${element.name}</a>`;
        addRaw([link,'Action',element.type,element.description],appElementsTable);
    });
}

function initIndexPage(){
    populateJobNames();
}

function applicationDetails(){
    var jobName = document.getElementById('jobNames').value;
    var endPoint = getAppDetailsUrl(jobName)
    makeAPICall(endPoint,'GET',null,setApplicationDetails)
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
    var jobDetails = responseData.data;
    document.getElementById('jobNameLabel').textContent = jobDetails.name;
    document.getElementById('appIdLabel').textContent = jobDetails.application_id;
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
        addRaw([paramName,'-',jobParameters[paramName]],jobParametersTable);
        addRaw([paramName,jobParameters[paramName]],jobRunParametersTable);
    });
}

function initJobDetailsPage(){
    var jobName=getQueryParam('name');
    if (name === null) {
        console.log("Query parameter 'name' is not present.");
        return;
    }
    var endPoint = getJobDetailsUrl(jobName)
    makeAPICall(endPoint,'GET',null,setJobDetails)
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

function getJobRunUrl(){
  return BASE_URL+"/jobs/run/";
}


function runJob(){
    var jobName = document.getElementById('jobNameLabel').textContent.trim();;
        console.log("jobName - "+jobName)

    var jobParameters = readTableParameters();

    var url=getJobRunUrl()
    var requestData={
                    jobName: jobName,
                    jobParameters:JSON.stringify(jobParameters)
                    };
    makeAPICall(url,'POST',requestData,function(response){
        $('#runJobModal').modal('hide');
        document.getElementById("messageModalBody").textContent = `Job '${jobName}' has been started successfully.`;
        $('#messageModal').modal('show');
    })
}

function openJobHistoryPage(){
    var jobName = document.getElementById('jobNames').value;
    window.open(`/job_history?name=${encodeURIComponent(jobName)}`, "_blank");
}

function getJobCurrentStatusUrl(jobName){
  return BASE_URL+"/jobs/status/"+jobName
}

function setJobCurrentStatus(responseData){
    var jobStatus = responseData.data;
    var jobName=getQueryParam('name');

    document.getElementById('jobNameLabel').textContent = jobName;
    document.getElementById('appIdLabel').textContent = jobStatus.app_id;
    document.getElementById('jobRunByLabel').textContent = jobStatus.run_by;
    document.getElementById('jobRunTypeLabel').textContent = jobStatus.run_type;
    document.getElementById('jobStatusLabel').textContent = jobStatus.status;
    document.getElementById('jobStartDateLabel').textContent = jobStatus.start_time;
    document.getElementById('jobEndDateLabel').textContent = jobStatus.end_time;
    document.getElementById('jobMessageLabel').textContent = jobStatus.message;
}

function setJobHistory(responseData){
    var jobHistory = responseData.data;
    console.log(JSON.stringify(jobHistory, null, 2))

    var jobExeDetailsTable = document.getElementById('jobExeDetailsTable')
    clearTableContents(jobExeDetailsTable);

     jobHistory.forEach(element=>{
        addRaw([element.run_by,element.run_type,element.status,element.start_time,element.end_time,element.message],jobExeDetailsTable);
    });

}

function getJobHistoryUrl(jobName){
  return BASE_URL+"/jobs/history/"+jobName
}

function populateJobCurrentStatus(jobName){
    var statusEndPoint = getJobCurrentStatusUrl(jobName);
    makeAPICall(statusEndPoint,'GET',null,setJobCurrentStatus);
}

function populateJobHistory(jobName){
    var historyEndPoint=getJobHistoryUrl(jobName);
    makeAPICall(historyEndPoint,'GET',null,setJobHistory);
}

function initHistoryPage(){
    var jobName = getQueryParam('name');
    if (name === null) {
        console.log("Query parameter 'name' is not present.");
        return;
    }

    populateJobCurrentStatus(jobName);
    populateJobHistory(jobName);



}