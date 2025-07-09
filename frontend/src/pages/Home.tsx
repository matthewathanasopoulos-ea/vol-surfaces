import React, { useState, useEffect } from "react";
import PriceChart from "../components/PriceChart";
import { Button } from "antd";

const Home = () => {
  const [error, setError] = useState<string>("");

  return (
    <div style={{ padding: "20px" }}>
      <Button onClick={() => (window.location.href = "/writing-helper")}>
        Writing Helper Tool
      </Button>

      <div style={{ padding: "20px" }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "20px" }}>
          <PriceChart asset="gold" title="Gold Price (USD/oz)" color="#FFD700" />
          <PriceChart asset="silver" title="Silver Price (USD/oz)" color="#C0C0C0" />
          <PriceChart asset="platinum" title="Platinum Price (USD/oz)" color="#E5E4E2" />
          <PriceChart asset="palladium" title="Palladium Price (USD/oz)" color="#E6E8FA" />
          <PriceChart asset="copper" title="Copper Price (USD/lb)" color="#B87333" />
          <PriceChart asset="aluminium" title="Aluminium Price (USD/tonne)" color="#A9A9A9" />
        </div>

        {error !== "" && (
          <div className="error-container">
            <h2>Error:</h2>
            <p>{error}</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default Home;
