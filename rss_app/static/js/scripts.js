let currentPage = 1;
const limit = 10;

function renderDocument(document) {
    let html = `
        <div class="card mb-3">
            <div class="card-body">
                <h5 class="card-title">${document.title}</h5>
                <h6 class="card-subtitle mb-2 text-muted">${new Date(document.datetime).toLocaleString()}</h6>
                <p class="card-text">${document.text}</p>
                ${document.image ? `<img src="${document.image}" alt="Image" class="img-fluid">` : ''}
            </div>
        </div>`;
    $('#documents').prepend(html);
}

function fetchDocuments(page, limit) {
    $.getJSON(`/get_documents?page=${page}&limit=${limit}`, function(data) {
        data.forEach(doc => renderDocument(doc));
    });
}

$(document).ready(function() {
    // Initial fetch
    fetchDocuments(currentPage, limit);

    // Load more documents on button click
    $('#load-more').click(function() {
        currentPage += 1;
        fetchDocuments(currentPage, limit);
    });

    // Set up SSE for real-time updates
    const eventSource = new EventSource("/stream");
    eventSource.addEventListener("new_document", function(event) {
        renderDocument(JSON.parse(event.data));
    });
});