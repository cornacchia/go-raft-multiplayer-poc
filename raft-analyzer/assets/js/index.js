import 'bootstrap'
import 'bootstrap/dist/css/bootstrap.min.css';
import _ from 'lodash'

function component() {
  const element = document.createElement('div')

  // Lodash, currently included via a script, is required for this line to work
  element.innerHTML = _.join(['Hello', 'world'], ' ')

  return element;
}

document.body.appendChild(component())