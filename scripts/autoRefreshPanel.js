/////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Author: Nicolas Chourot
// 2026
//
// Dependances :
//     - jquery version > 3.0
//     - bootbox
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////let EndSessionAction = '/Accounts/Login'; 

let DefaultPeriodicRefreshRate = 15 /* 15 seconds */;

class AutoRefreshedPanel {
    constructor(panelId, contentServiceURL, refreshRate = DefaultPeriodicRefreshRate, postRefreshCallback = null) {
        this.contentServiceURL = contentServiceURL;
        this.panelId = panelId;
        this.postRefreshCallback = postRefreshCallback;

        if (refreshRate != -1) { // will be refreshed manually
            this.refresh(true);
            this.refreshRate = refreshRate * 1000; /* convert in miliseconds */
            this.paused = false;
            setInterval(() => { this.refresh() }, this.refreshRate);
        }
    }
    pause() {
        this.paused = true;
    }
    restart() {
        this.paused = false
    }
    replaceContent(htmlContent) {
        if (htmlContent !== "") {
            $("#" + this.panelId).html(htmlContent);
            console.log(`Panel ${this.panelId} has been refreshed.`);
            if (this.postRefreshCallback != null) this.postRefreshCallback();
        }
    }
    refresh(forced = false) {
        if (!this.paused) {
            $.ajax({
                url: this.contentServiceURL + (forced ? (this.contentServiceURL.indexOf("?") > -1 ? "&" : "?") + "forceRefresh=true" : ""),
                dataType: "html",
                success: (htmlContent) => {
                    if (htmlContent != "blocked")
                        this.replaceContent(htmlContent);
                },
                statusCode: {
                    401: function () {
                        if (EndSessionAction != "")
                            window.location = EndSessionAction + "?message=Votre session a été fermée!&success=false";
                        else
                            alert("Illegal access!");
                    }
                }
            })
        }
    }
    command(url, moreCallBack = null) {
        $.ajax({
            url: url,
            method: 'GET',
            success: (params) => {
                this.refresh(true);
                if (moreCallBack != null)
                    moreCallBack(params);

            },
            statusCode: {
                500: function () {
                    if (EndSessionAction != "")
                        window.location = EndSessionAction + "?message=Votre session a été fermée!&success=false";
                    else
                        alert("Illegal access!");
                }
            }
        });
    }
    postCommand(url, data, moreCallBack = null) {
        $.ajax({
            url: url,
            method: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(data),
            success: (params) => {
                this.refresh(true);
                if (moreCallBack != null)
                    moreCallBack(params);
            },
            statusCode: {
                500: function () {
                    if (EndSessionAction != "")
                        window.location = EndSessionAction + "?message=Votre session a été fermée!&success=false";
                    else
                        alert("Illegal access!");
                }
            }
        });
    }

    confirmedCommand(message, url, moreCallBack = null) {
        bootbox.confirm(message, (result) => { if (result) this.command(url, moreCallBack) });
    }
}
