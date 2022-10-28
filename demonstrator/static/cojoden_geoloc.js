var lat = document.getElementById("coord_lat");
var long = document.getElementById("coord_long");
var error_zone = document.getElementById("errors");

function getLocation() {
  error_zone.innerHTML = "";
  if (navigator.geolocation) {
    navigator.geolocation.watchPosition(showPosition);
  } else { 
    error_zone.innerHTML = "Geolocation is not supported by this browser.";
  }
}
    
function showPosition(position) {
    lat.value = position.coords.latitude;
    long.value = position.coords.longitude;
}