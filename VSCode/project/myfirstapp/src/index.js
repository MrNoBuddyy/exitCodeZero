import React from 'react';
import ReactDOM from 'react-dom/client';

import App from './App';
//import reportWebVitals from './reportWebVitals';
//<img src="https://www.icloud.com/sharedalbum/#B0NGWZuqD1J0eLw"
//alt="mall pics"
///>
const root = ReactDOM.createRoot(document.getElementById('root'));
function ref(){
root.render(<App />);
}
setInterval(ref,1000);
// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
//reportWebVitals();
