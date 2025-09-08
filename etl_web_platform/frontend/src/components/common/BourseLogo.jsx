import React from 'react';
import { motion } from 'framer-motion';
import logoImage from '../../assets/bvmt-logo.png';

export function BourseLogo({ size = "w-8 h-8", className = "" }) {
  return (
    <motion.img 
      src={logoImage} 
      alt="BVMT - Bourse des Valeurs Mobilières de Tunis" 
      className={`${size} object-contain ${className}`}
      whileHover={{ scale: 1.1, rotate: 2 }}
      transition={{ duration: 0.3 }}
    />
  );
}
