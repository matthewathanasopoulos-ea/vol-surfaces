import React, { useState, useEffect } from "react";
import PriceChart from "./components/PriceChart";

interface PingResponse {
  message: string;
  status: number;
}

function App(): React.JSX.Element {
  const [pingResponse, setPingResponse] = useState<string>("");
  const [error, setError] = useState<string>("");

  const handleRequest = async (): Promise<void> => {
    setError("");

    try {
      const response = await fetch("http://127.0.0.1:5000/ping");

      if (!response.ok) {
        setError(`HTTP error! status: ${response.status}`);
      }

      const data: PingResponse = await response.json();

      setPingResponse(data["message"]);

      console.log(pingResponse);
      console.log("Response from backend:", data);
    } catch (err) {
      console.error("Error connecting to backend:", err);
    }
  };

  return (
    <div style={{ padding: "20px" }}>
      <h1>Matthew EA Charts</h1>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "20px" }}>
        <PriceChart asset="gold" title="Gold Price (USD/oz)" color="#FFD700" />
        <PriceChart asset="silver" title="Silver Price (USD/oz)" color="#C0C0C0" />
        <PriceChart asset="platinum" title="Platinum Price (USD/oz)" color="#E5E4E2" />
        <PriceChart asset="palladium" title="Palladium Price (USD/oz)" color="#E6E8FA" />
        <PriceChart asset="copper" title="Copper Price (USD/lb)" color="#B87333" />
        <PriceChart asset="aluminium" title="Aluminium Price (USD/tonne)" color="#A9A9A9" />
      </div>
      <h1>3D Volatility Surfaces</h1>
      <div>
        <header className="App-header">
          <h1>Frontend App</h1>
          <div style={{ paddingLeft: "100px" }}>
            <button onClick={handleRequest}>Ping</button>
          </div>

          {pingResponse && (
            <div className="response-container">
              <p>{pingResponse}</p>
            </div>
          )}

          {error !== "" && (
            <div className="error-container">
              <h2>Error:</h2>
              <p>{error}</p>
            </div>
          )}
        </header>
      </div>
    </div>
  );
}

export default App;
