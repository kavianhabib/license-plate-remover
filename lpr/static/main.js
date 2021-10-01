Dropzone.autoDiscover = false;

const myDropzone = new Dropzone("#my-dropzone", {
    // url:"{{session.get_upload_url}}",
    maxFiles: 2,
    maxFilesize: 5,
    acceptedFiles: '.jpg, .png',
})