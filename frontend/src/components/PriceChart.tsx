import React, { useState, useEffect } from "react";
import Plot from "react-plotly.js";

interface PriceChartProps {
  asset: string;
  title?: string;
  color?: string;
  height?: number;
  apiBaseUrl?: string;
}

const PriceChart: React.FC<PriceChartProps> = ({
  asset,
  title,
  color = "#17BECF",
  height = 300,
  apiBaseUrl = "http://127.0.0.1:5000",
}) => {
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [plotData, setPlotData] = useState<any[]>([]);
  const [layout, setLayout] = useState<any>({});

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);

      try {
        const response = await fetch(`${apiBaseUrl}/continous_contract/${asset}`);

        if (!response.ok) {
          throw new Error(`Failed to fetch data: ${response.statusText}`);
        }

        const result = await response.json();

        const parsedData = JSON.parse(result.data);
        const dates = Object.keys(parsedData);
        const values = dates.map((date) => parsedData[date].val);

        setPlotData([
          {
            x: dates,
            y: values,
            type: "scatter",
            mode: "lines",
            name: asset.charAt(0).toUpperCase() + asset.slice(1).replace(/_/g, " "),
            line: { color: color },
          },
        ]);

        setLayout({
          title:
            title || `${asset.charAt(0).toUpperCase() + asset.slice(1).replace(/_/g, " ")} Price`,
          xaxis: {
            title: "Date",
            rangeslider: { visible: true },
          },
          yaxis: {
            title: "Price",
          },
          autosize: true,
          margin: { l: 40, r: 20, b: 20, t: 20, pad: 4 },
        });

        setIsLoading(false);
      } catch (error) {
        console.error("Error fetching data:", error);
        setError(error instanceof Error ? error.message : "An unknown error occurred");
        setIsLoading(false);
      }
    };

    fetchData();
  }, [asset, title, color, apiBaseUrl]);

  if (error) {
    return (
      <div className="error-container">
        <h2>Error Loading Chart</h2>
        <p>{error}</p>
      </div>
    );
  }

  return (
    <div className="price-chart-container">
      <h2>
        {title || `${asset.charAt(0).toUpperCase() + asset.slice(1).replace(/_/g, " ")} Price`}
      </h2>
      {isLoading ? (
        <div className="loading-indicator">Loading chart data...</div>
      ) : (
        <Plot
          data={plotData}
          layout={layout}
          style={{ width: "100%", height: `${height}px` }}
          useResizeHandler={true}
        />
      )}
    </div>
  );
};

export default PriceChart;
