let currentPage = 1;
const limit = 10;

function renderDocument(document) {
    let html = `
        <div class="card mb-3 data-id="${document._id}">
            <div class="card-body">
                <h5 class="card-title">${document.title}</h5>
                <h6 class="card-subtitle mb-2 text-muted">${new Date(document.datetime).toLocaleString()}</h6>
                <p class="card-text">${document.text}</p>
                ${document.image ? `<img src="${document.image}" alt="Image" class="img-fluid">` : ''}
            </div>
        </div>`;
    $('#documents').prepend(html);
}

function renderDocuments(documents, clear = false) {
    if (clear) {
        $('#documents').empty();
    }
    documents.forEach(doc => renderDocument(doc));
}

function fetchDocuments(page, limit) {
    $.getJSON(`/get_documents?page=${page}&limit=${limit}`, function(data) {
        renderDocuments(data, clear);
    });
}

function checkForNewDocuments() {
    const firstDocumentId = $('#documents').children().first().attr('data-id');
    $.getJSON('/latest_document', function(data) {
        if (data && data._id !== firstDocumentId) {
            $('#new-documents-indicator').show();
        }
    });
}

$(document).ready(function() {
    // Initial fetch
    fetchDocuments(currentPage, limit, true);

    // Load more documents on button click
    $('#load-more').click(function() {
        currentPage += 1;
        fetchDocuments(currentPage, limit);
    });

    // Refresh feed on button click
    $('#refresh-feed').click(function() {
        fetchDocuments(1, limit, true);
        currentPage = 1;
    });

    // Refresh now button inside the indicator
    $('#refresh-now').click(function() {
        fetchDocuments(1, limit, true);
        currentPage = 1; // Reset the current page to 1
        $('#new-documents-indicator').hide(); // Hide the indicator after refreshing
    });

    // Periodically check for new documents without refreshing the feed
    setInterval(checkForNewDocuments, 10000); // Check every 10 seconds
});