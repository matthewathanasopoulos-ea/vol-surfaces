import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Home from "./pages/Home";
import WritingHelper from "./pages/WritingHelper";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/writing-helper" element={<WritingHelper />} />
      </Routes>
    </Router>
  );
}

export default App;
