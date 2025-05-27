

function App(props) {
  const currDate = new Date();
  return (
    <div>
    <h1>Hello Buddy! vijay here </h1>
    <h2>The time now is {currDate.toLocaleTimeString()}</h2>
    <h3><b>The date of the day is {currDate.toLocaleDateString()}</b></h3>
    
    
    </div>
  );
}

export default App;
