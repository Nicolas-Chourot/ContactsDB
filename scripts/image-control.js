/////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Author: Nicolas Chourot
// 2026
//
// Dependance : jquery version > 3.0
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// This script generate necessary html control in order to offer an image uploader.
// Also it include validation rules to avoid submission on empty file and excessive image size.
//
// This script is dependant of jquery and jquery validation.
//
//  Any <div> written as follow will contain an image file uploader :
//
//  <div class='imageUploader' id='data_Id' controlId = 'controlId' imageSrc='image url'> </div>
//  <span class="field-validation-valid text-danger" data-valmsg-for="controlId" data-valmsg-replace="true"></span>
//
//  If id = 0 the file not empty validation rule will be applied
//
//  Example:
//
//  With the following:
/*
    <div    class='imageUploader' 
            controlId='PhotoImageData' 
            imageSrc='Default_image.png' 
            title='Click to select an image'>
    </div>
*/
//  We obtain:
//  <div class="imageUploader" id="0"
//       controlid="PhotoImageData"
//       imagesrc="No_image.png"
//       waitingImage = "Loading_icon.gif" >
//
//      <!-- Image uploaded -->
//      <img id="PhotoImageData_UploadedImage"
//           name="PhotoImageData_UploadedImage"
//           class="UploadedImage"
//           src="Default_image.png">
//
//      <!-- hidden file uploader -->
//      <input id="PhotoImageData_ImageUploader"
//             type="file"
//             style="visibility:hidden; height:0px;"
//             accept="image/jpeg,image/gif,image/png,image/bmp">
//
//      <!-- hidden input uploaded imageData container -->
//      <input style="height:0px; width:0px; border:1px solid white;"
//             id="PhotoImageData"
//             name="PhotoImageData"
//             waitingImage="Loading_icon.gif">
//  </div>
//  <span class="field-validation-valid text-danger" data-valmsg-for="PhotoImageData" data-valmsg-replace="true"></span>
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Error messages
// let missingFileErrorMessage = "You must select an image file.";
// let tooBigFileErrorMessage = "Image too big! Please choose another one.";
// let wrongFileFormatMessage = "It is not a valid image file. Please choose another one.";

let missingFileErrorMessage = "Veuillez sélectionner une image.";
let tooBigFileErrorMessage = "L'image est trop volumineuse.";
let wrongFileFormatMessage = "Ce format d'image n'est pas accepté.";
let maxImageSize = 15000000;
var currentId = 0;
let initialImageURL = "";
let waitingImage = "/App_Assets/Loading_icon.gif";

// Accepted file formats
let acceptedFileFormat = "image/jpeg,image/jpg,image/gif,image/png,image/bmp,image/webp,image/avif, image/svg";

$(document).ready(() => {
    /* you can have more than one file uploader */
    initImageUploaders();
});

function initImageUploaders() {
    $('.imageUploader').each(function () {
        let controlId = $(this).attr('controlId');
        let title = $(this).attr('title');
        let requireMessage = $(this).attr('requireMessage');
        missingFileErrorMessage = requireMessage;
        $(this).css("display", "flex");
        $(this).css("flex-direction", "column");
        $(this).css("align-items", "center");
        $(this).css("border-radius", "6px");
        $(this).css("padding", "6px");
        $(this).css("padding-left", "0px");
        $(this).css("padding-bottom", "3px");

        let imageData = $(this).attr('imageSrc');
        initialImageURL = imageData;

        $(this).append(`<img 
                         id="${controlId}_UploadedImage" 
                         tabindex=0 
                         class="UploadedImage"
                         style="width:100%"
                         src="${imageData}"
                         title="${title}";
                         waitingImage ="${waitingImage}">`);

        $(this).append(`<input 
                         id="${controlId}_ImageUploader" 
                         type="file" style="visibility:hidden; height:0px; width:0px; border:none; margin:0px !important"
                         accept="${acceptedFileFormat}">`);

        $(this).append(`<input 
                        style="visibility:hidden;height:0px;" 
                        class="fileUploadedSizeRule"
                        id="${controlId}" 
                        name="${controlId}" 
                        waitingImage ="${waitingImage}"
                        value="${imageData}">`);

        //$.validator.addMethod("fileUploadedSizeRule", function (value, element) { return CheckPhotoSize(element); }, tooBigFileErrorMessage);

        $(`#${controlId}_UploadedImage`).on('dragenter', function (e) {
            $(this).css('border', '2px solid blue');
        });

        $(`#${controlId}_UploadedImage`).on('dragover', function (e) {
            $(this).css('border', '2px solid blue');
            e.preventDefault();
        });

        $(`#${controlId}_UploadedImage`).on('dragleave', function (e) {
            $(this).css('border', '2px solid white');
            e.preventDefault();
        });

        $(`#${controlId}_UploadedImage`).on('drop', function (e) {
            var image = e.originalEvent.dataTransfer.files[0];
            $(this).css('background', '#D8F9D3');
            e.preventDefault();
            let id = $(this).attr('id').split('_')[0];
            let UploadedImage = document.querySelector('#' + id + '_UploadedImage');
            let waitingImage = UploadedImage.getAttribute("waitingImage");
            let ImageData = document.querySelector('#' + id);
            // store the previous uploaded image in case the file selection is aborted
            UploadedImage.setAttribute("previousImage", UploadedImage.src);

            // set the waiting image
            if (waitingImage !== "") UploadedImage.src = waitingImage;
            /* take some delai before starting uploading process in order to let browser to update UploadedImage new source affectation */
            let t2 = setTimeout(function () {
                if (UploadedImage !== null) {
                    let len = image.name.length;

                    if (len !== 0) {
                        let fname = image.name;
                        let ext = fname.split('.').pop().toLowerCase();

                        if (!validExtension(ext)) {
                            alert(wrongFileFormatMessage);
                            UploadedImage.src = UploadedImage.getAttribute("previousImage");
                        }
                        else {
                            let fReader = new FileReader();
                            fReader.readAsDataURL(image);
                            fReader.onloadend = () => {
                                UploadedImage.src = fReader.result;
                                ImageData.value = initialImageURL + "|" + UploadedImage.src;
                                ImageData.setCustomValidity('');
                            };
                        }
                    }
                    else {
                        UploadedImage.src = null;
                    }
                }
            }, 30);
            $(this).css('border', '2px solid white');
            return true;
        });
        ImageUploader_AttachEvent(controlId);
        let controlIdTop = - $(this).height() / 2;
        let controlIdLeft = 4;
        $(`#${controlId}`).css("z-index", "-1");
        $(`#${controlId}`).css("height", "0px");
        $(`#${controlId}`).css("width", "0px");
        $(`#${controlId}`).css("margin", "0px");
        $(`#${controlId}`).css("position", "relative");
        $(`#${controlId}`).css("left", `${controlIdLeft}px`);
        $(`#${controlId}`).css("top", `${controlIdTop}px`);
    });
}

function rotateBase64Image90deg(controlId, clockwise = false) {
    let domId = `${controlId}_UploadedImage`;

    // create an off-screen canvas
    var offScreenCanvas = document.createElement('canvas');
    offScreenCanvasCtx = offScreenCanvas.getContext('2d');

    // cteate Image
    var img = new Image();
    img.src = $("#" + domId).attr('src');

    // set its dimension to rotated size
    offScreenCanvas.height = img.width;
    offScreenCanvas.width = img.height;

    if (clockwise) {
        offScreenCanvasCtx.rotate(90 * Math.PI / 180);
        offScreenCanvasCtx.translate(0, -offScreenCanvas.width);
    } else {
        offScreenCanvasCtx.rotate(-90 * Math.PI / 180);
        offScreenCanvasCtx.translate(-offScreenCanvas.height, 0);
    }
    offScreenCanvasCtx.drawImage(img, 0, 0);

    let base64 = offScreenCanvas.toDataURL("image/jpeg", 100);
    // encode image to data-uri with base64
    $("#" + domId).attr('src', base64);
    $("#" + controlId).val(initialImageURL + "|" + base64);
}

function resizeCrop(src, width, height) {
    var crop = width == 0 || height == 0;
    // not resize
    if (src.width <= width && height == 0) {
        width = src.width;
        height = src.height;
    }
    // resize
    if (src.width > width && height == 0) {
        height = src.height * (width / src.width);
    }

    // check scale
    var xscale = width / src.width;
    var yscale = height / src.height;
    var scale = crop ? Math.min(xscale, yscale) : Math.max(xscale, yscale);
    // create empty canvas
    var canvas = document.createElement("canvas");
    canvas.width = width ? width : Math.round(src.width * scale);
    canvas.height = height ? height : Math.round(src.height * scale);
    canvas.getContext("2d").scale(scale, scale);
    // crop it top center
    canvas.getContext("2d").drawImage(src, ((src.width * scale) - canvas.width) * -.5, ((src.height * scale) - canvas.height) * -.5);
    return canvas;
}

// Check if uploaded image exceed maximum sixe
function CheckPhotoSize(element) {
    var files = $("#" + $(element).attr('id') + "_ImageUploader").get(0).files;
    if (files.length > 0)
        return files[0].size < maxImageSize;
    else
        return true;
}

function ImageUploader_AttachEvent(controlId) {
    // one click will be transmitted to #ImageUploader
    document.querySelector('#' + controlId + '_UploadedImage').
        addEventListener('click', () => {
            document.querySelector('#' + controlId + '_ImageUploader').click();
        });
    document.querySelector('#' + controlId + '_ImageUploader').addEventListener('change', preLoadImage);
}

function validExtension(ext) {
    return acceptedFileFormat.indexOf("/" + ext) > 0;
}

function preLoadImage(event) {
    // extract the id of the event target
    let id = event.target.id.split('_')[0];
    let UploadedImage = document.querySelector('#' + id + '_UploadedImage');
    let waitingImage = UploadedImage.getAttribute("waitingImage");
    let ImageUploader = document.querySelector('#' + id + '_ImageUploader');
    let ImageData = document.querySelector('#' + id);
    // store the previous uploaded image in case the file selection is aborted
    UploadedImage.setAttribute("previousImage", UploadedImage.src);
    // is there a file selection
    if (ImageUploader.value.length > 0) {

        // set the waiting image
        if (waitingImage !== "") UploadedImage.src = waitingImage;
        /* take some delai before starting uploading process in order to let browser to update UploadedImage new source affectation */
        let t2 = setTimeout(function () {
            if (UploadedImage !== null) {
                let len = ImageUploader.value.length;

                if (len !== 0) {
                    let fname = ImageUploader.value;
                    let ext = fname.split('.').pop().toLowerCase();

                    if (!validExtension(ext)) {
                        alert(wrongFileFormatMessage);
                        UploadedImage.src = UploadedImage.getAttribute("previousImage");
                    }
                    else {
                        let fReader = new FileReader();
                        fReader.readAsDataURL(ImageUploader.files[0]);
                        fReader.onloadend = () => {
                            UploadedImage.src = fReader.result;
                            ImageData.value = initialImageURL + "|" + UploadedImage.src;
                            ImageData.setCustomValidity('');
                        };
                    }
                }
                else {
                    UploadedImage.src = null;
                }
            }
        }, 30);
    }
    return true;
}

document.onpaste = function (event) {
    try {
        let id = event.target.id.split('_')[0];
        let UploadedImage = document.querySelector('#' + id + '_UploadedImage');
        if (UploadedImage) {
            let ImageData = document.querySelector('#' + id);
            let waitingImage = UploadedImage.getAttribute("waitingImage");
            if (waitingImage !== "") UploadedImage.src = waitingImage;
            // use event.originalEvent.clipboard for newer chrome versions
            var items = (event.clipboardData || event.originalEvent.clipboardData).items;
            // find pasted image among pasted items
            var blob = null;
            for (var i = 0; i < items.length; i++) {
                if (items[i].type.indexOf("image") === 0) {
                    blob = items[i].getAsFile();
                }
            }
            // load image if there is a pasted image
            if (blob !== null) {
                var reader = new FileReader();
                reader.onload = function (event) {
                    UploadedImage.src = event.target.result;
                    ImageData.value = initialImageURL + "|" + UploadedImage.src;
                    ImageData.setCustomValidity('');
                };
                reader.readAsDataURL(blob);
            }
        }
    }
    catch (error) {
        console.log(error);
    }
}

//https://soshace.com/the-ultimate-guide-to-drag-and-drop-image-uploading-with-pure-javascript/