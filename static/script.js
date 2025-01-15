document.addEventListener('DOMContentLoaded', async () => {
    const companySelect = document.getElementById('company');
    const form = document.getElementById('data-form');
    const tableContainer = document.getElementById('table-container');

    // Fetch company options
    async function loadCompanyOptions() {
        try {
            const response = await fetch('/company-options');
            const data = await response.json();

            if (data.error) {
                alert(`Error: ${data.error}`);
                return;
            }

            data.company_options.forEach(option => {
                const opt = document.createElement('option');
                opt.value = option.value;
                opt.textContent = option.label;
                companySelect.appendChild(opt);
            });
        } catch (error) {
            console.error('Error fetching company options:', error);
        }
    }

    // Handle form submission
    form.addEventListener('submit', async event => {
        event.preventDefault();
        const selectedCompany = companySelect.value;
        const selectedStatement = document.getElementById('statement').value;

        if (!selectedCompany) {
            alert('Please select a company.');
            return;
        }

        try {
            const response = await fetch(`/financial-data?selected_company=${encodeURIComponent(selectedCompany)}&selected_statement_type=${encodeURIComponent(selectedStatement)}`);
            const data = await response.json();

            tableContainer.innerHTML = '';

            if (data.error) {
                tableContainer.innerHTML = `<p style="color: red;">Error: ${data.error}</p>`;
                return;
            }

            if (data.message) {
                tableContainer.innerHTML = `<p>${data.message}</p>`;
                return;
            }

            // Render table
            const table = document.createElement('table');
            const thead = document.createElement('thead');
            const tbody = document.createElement('tbody');

            // Add headers
            const headers = Object.keys(data.financial_data[0]);
            const headerRow = document.createElement('tr');
            headers.forEach(header => {
                const th = document.createElement('th');
                th.textContent = header;
                headerRow.appendChild(th);
            });
            thead.appendChild(headerRow);

            // Add rows
            data.financial_data.forEach(row => {
                const tr = document.createElement('tr');
                headers.forEach(header => {
                    const td = document.createElement('td');
                    td.textContent = row[header];
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });

            table.appendChild(thead);
            table.appendChild(tbody);
            tableContainer.appendChild(table);
        } catch (error) {
            console.error('Error fetching financial data:', error);
            tableContainer.innerHTML = `<p style="color: red;">Failed to fetch data. Please try again later.</p>`;
        }
    });

    // Initialize company options
    await loadCompanyOptions();
});