
import React from 'react';
import { useParams } from 'react-router-dom';

const StrategyDetails = () => {
    const { id } = useParams();
    return (
        <div className="p-6">
            <h1 className="text-2xl font-bold mb-4">Strategy Details: {id}</h1>
            <p>This module is currently under development.</p>
        </div>
    );
};

export default StrategyDetails;
